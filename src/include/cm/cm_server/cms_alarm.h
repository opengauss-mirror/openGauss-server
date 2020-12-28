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
 * cms_alarm.h
 *
 * IDENTIFICATION
 *	  src/include/cm/cm_server/cms_alarm.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_ALARM_H
#define CMS_ALARM_H

#include "alarm/alarm.h"

typedef enum CM_DiskPreAlarmType {
    PRE_ALARM_LOG = 0,
    PRE_ALARM_CN = 1,
    PRE_ALARM_DN = 2
} CM_DiskPreAlarmType;

typedef struct instance_phony_dead_alarm {
    uint32 instanceId;
    Alarm PhonyDeadAlarmItem[1];
} instance_phony_dead_alarm;

extern void StorageThresholdPreAlarmItemInitialize(void);
extern void ReportStorageThresholdPreAlarm(
    AlarmType alarmType,
    const char* instanceName,
    CM_DiskPreAlarmType alarmNode,
    uint32 alarmIndex);
extern void ReadOnlyAlarmItemInitialize(void);
extern void ReportReadOnlyAlarm(AlarmType alarmType, const char* instanceName, uint32 alarmIndex);
extern void PhonyDeadAlarmItemInitialize(void);
extern void report_phony_dead_alarm(AlarmType alarmType, const char* instanceName, uint32 instanceid);
extern void report_unbalanced_alarm(AlarmType alarmType);


extern void EtcdAbnormalAlarmItemInitialize(void);
extern void UnbalanceAlarmItemInitialize(void);
extern void ServerSwitchAlarmItemInitialize(void);
extern void report_server_switch_alarm(AlarmType alarmType, const char* instanceName);
void report_etcd_fail_alarm(AlarmType alarmType, char* instanceName, int alarmIndex);

#endif