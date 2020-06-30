/* ---------------------------------------------------------------------------------------
 * 
 * alarmchecker.h
 *        POSTGRES alarm reporting/logging definitions.
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
