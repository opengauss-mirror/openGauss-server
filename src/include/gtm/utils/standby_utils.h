/* -------------------------------------------------------------------------
 *
 * standby_utils.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/standby_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STANDBY_UTILS_H
#define STANDBY_UTILS_H

#include "gtm/gtm_c.h"

bool Recovery_IsPrimary(void);
bool Recovery_IsStandby(void);
bool Recovery_IsPending(void);
bool Recovery_InProgress(void);
GTMServerMode Recovery_Mode(void);

void Recovery_StandbySetStandby(GTMServerMode mode);
void Recovery_InitStandbyLock(void);
void Recovery_SetPosition(int HA, int local);
void Recovery_GetPosition(int* HA, int* local);

#endif
