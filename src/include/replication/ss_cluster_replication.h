/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * Description: openGauss is licensed under Mulan PSL v2.
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
 *
 *
 * IDENTIFICATION
 *        src/include/replication/ss_cluster_replication.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INCLUDE_REPLICATION_SS_CLUSTER_REPLICATION_H_
#define INCLUDE_REPLICATION_SS_CLUSTER_REPLICATION_H_

#include "postgres.h"
#include "replication/walprotocol.h"
#include "knl/knl_instance.h"
#include <vector>

const uint32 SS_DORADO_CTL_INFO_SIZE = 512;

#define SS_CLUSTER_DORADO_REPLICATION \
        (ENABLE_DSS && g_instance.attr.attr_storage.enable_ss_dorado)

/* Primary Cluster in SS replication */
#define SS_REPLICATION_MAIN_CLUSTER \
        (SS_CLUSTER_DORADO_REPLICATION && (g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_PRIMARY))

/* Standby Cluster in SS replication */
#define SS_REPLICATION_STANDBY_CLUSTER \
        (SS_CLUSTER_DORADO_REPLICATION && (g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_STANDBY))

/* Primary node in SS replication, means primary node in main cluster. */
#define IS_SS_REPLICATION_PRIMARY_NODE \
    (SS_REPLICATION_MAIN_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE))

/* Standby node in SS replication, means standby node in main cluster. */
#define IS_SS_REPLICATION_PRIMARY_CLUSTER_STANDBY_NODE \
    (SS_REPLICATION_MAIN_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* Main standby node in SS replication, means primary node in standby cluster. */
#define IS_SS_REPLICATION_MAIN_STANBY_NODE \
    (SS_REPLICATION_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE))

/* Standby node in SS replication, means standby node in standby cluster. */
#define IS_SS_REPLICATION_STANDBY_CLUSTER_STANDBY_NODE \
    (SS_REPLICATION_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* All Standby in SS replication, means nodes other than primary node in primary cluster and standby cluster */
#define IS_SS_REPLICATION_STANBY_NODE \
    (SS_CLUSTER_DORADO_REPLICATION && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))


void SSClusterDoradoStorageInit();
void InitSSDoradoCtlInfo(ShareStorageXLogCtl *ctlInfo, uint64 sysidentifier);
void UpdateSSDoradoCtlInfoAndSync();
void WriteSSDoradoCtlInfoFile();
void ReadSSDoradoCtlInfoFile();
void CheckSSDoradoCtlInfo(XLogRecPtr localEnd);
#endif // INCLUDE_REPLICATION_SS_CLUSTER_REPLICATION_H_