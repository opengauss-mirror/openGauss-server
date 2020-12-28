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
 * cms_disk_check.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_disk_check.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_DISK_CHECK_H
#define CMS_DISK_CHECK_H

#include "common/config/cm_config.h"

#define INSTANCE_NAME_LENGTH 256

extern int InitNodeReadonlyInfo();
extern bool checkReadOnlyStatus(uint32 instanceId);

extern void* StorageDetectMain(void* arg);

extern void SetPreAlarmForNodeInstance(uint32 instanceId);
extern void ReportPreAlarmForNodeInstance();
extern void PreAlarmForNodeThreshold();

extern void SetPreAlarmForLogDiskInstance(const char* nodeName);
extern void RepostPreAlarmForLogDiskInstance();
extern void PreAlarmForLogPathThreshold();
extern void SetNodeInstanceReadOnlyStatus(int instanceType, uint32 instanceId, uint32 readonly);
extern void SaveNodeReadOnlyConfig();
extern void CheckAndSetStorageThresholdReadOnlyAlarm();

#endif