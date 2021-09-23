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
 * alarmchecker.h
 *        openGauss alarm reporting/logging definitions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/postmaster/alarmchecker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ALARMCHECKER_H
#define ALARMCHECKER_H
#include "alarm/alarm.h"

extern bool enable_alarm;

extern bool isDirExist(const char* dir);
extern ThreadId startAlarmChecker(void);
extern NON_EXEC_STATIC void AlarmCheckerMain();

#endif
