/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_init.h
 *  include header file for DMS shared storage.
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_init.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_DDES_SS_INIT_H
#define SRC_INCLUDE_DDES_SS_INIT_H

#include "c.h"

#define DMS_MAX_INSTANCE 64
#define DMS_MAX_SESSIONS (uint32)16320
#define DMS_MAX_CONNECTIONS (int32)16000

#define SS_PRIMARY_ID g_instance.dms_cxt.SSReformerControl.primaryInstId // currently master ID is hardcoded as 0
#define SS_RECOVERY_ID g_instance.dms_cxt.SSReformerControl.recoveryInstId
#define SS_MY_INST_ID g_instance.attr.attr_storage.dms_attr.instance_id
#define SS_OFFICIAL_PRIMARY (SS_MY_INST_ID == SS_PRIMARY_ID)
#define SS_OFFICIAL_RECOVERY_NODE (SS_MY_INST_ID == SS_RECOVERY_ID)

void DMSInit();
void DMSUninit();
int32 DMSWaitReform();
bool DMSWaitInitStartup();
void DMSInitLogger();
void DMSRefreshLogger(char *log_field, unsigned long long *value);
void GetSSLogPath(char *sslog_path);
void StartupWaitReform();

#endif
