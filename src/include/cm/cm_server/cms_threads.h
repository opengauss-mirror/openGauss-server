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
 * cms_threads.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_threads.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMS_THREADS_H
#define CMS_THREADS_H

#define SWITCHOVER_FLAG_FILE "cms_need_to_switchover"

extern int CM_CreateHA(void);
extern int CM_CreateMonitor(void);
extern int CM_CreateMonitorStopNode(void);
extern int CM_CreateThreadPool(int thrCount);

extern void CreateStorageThresholdCheckThread();
extern void create_deal_phony_alarm_thread();
extern void CreateAzStatusCheckForAzThread();
extern void CreateAz1Az2ConnectStateCheckThread();
extern void CreateDnGroupStatusCheckAndArbitrateThread();
extern void Init_cluster_to_switchover();
extern void CreateSyncDynamicInfoThread();
extern void CreateMultiAzConnectStateCheckThread();
extern void CreateCheckBlackListThread();
extern void* CheckBlackList(void* arg);
extern void CreateDynamicCfgSyncThread();
extern CM_Thread* GetNextThread(int ctlThreadNum);
extern CM_Thread* GetNextCtlThread(int threadNum);

#endif
