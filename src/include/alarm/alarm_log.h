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
 * alarm_log.h
 *        alarm logging and reporting
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/alarm_log.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ALARM_LOG_H
#define ALARM_LOG_H
#include "alarm/alarm.h"

#define SYSQUOTE "\""
#define SYSCOLON ":"
#define SYSCOMMA ","

#define MAX_BUF_SIZE 1024
extern char g_alarm_scope[MAX_BUF_SIZE];

void clean_system_alarm_log(const char* file_name, const char* sys_log_path);

void create_system_alarm_log(const char* sys_log_path);

void write_alarm(Alarm* alarmItem, const char* alarmName, const char* alarmLevel, AlarmType type,
    AlarmAdditionalParam* additionalParam);

void set_alarm_scope(const char* alarmScope);

#endif
