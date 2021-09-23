/* ---------------------------------------------------------------------------------------
 *
 * gtm_alarm.h
 *        openGauss alarm reporting/logging definitions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/gtm/gtm_alarm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GTM_ALARM_H
#define GTM_ALARM_H

extern bool enable_alarm;

extern int GTMAddAlarmCheckerThread(void);

#endif
