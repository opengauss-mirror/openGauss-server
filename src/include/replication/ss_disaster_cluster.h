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
 *        src/include/replication/ss_disaster_cluster.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INCLUDE_SS_DISASTER_CLUSTER_H_
#define INCLUDE_SS_DISASTER_CLUSTER_H_

#include "postgres.h"
#include "replication/walprotocol.h"
#include "knl/knl_instance.h"

/* stream cluster in share storage mode */
#define SS_STREAM_CLUSTER \
        (ENABLE_DSS && g_instance.attr.attr_storage.ss_stream_cluster)

/* Primary Cluster in SS disaster */
#define SS_STREAM_PRIMARY_CLUSTER \
        (SS_STREAM_CLUSTER && (g_instance.dms_cxt.SSReformerControl.clusterRunMode == RUN_MODE_PRIMARY))

/* Standby Cluster in SS disaster */
#define SS_STREAM_STANDBY_CLUSTER \
        (SS_STREAM_CLUSTER && (g_instance.dms_cxt.SSReformerControl.clusterRunMode == RUN_MODE_STANDBY))

/* Primary node in SS disaster, means primary node in main cluster. */
#define SS_STREAM_PRIMARY_NODE \
        (SS_STREAM_PRIMARY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE))

/* Standby node in SS disaster, means standby node in main cluster. */
#define SS_STREAM_PRIMARY_CLUSTER_STANDBY_NODE \
        (SS_STREAM_PRIMARY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* Main standby node in SS disaster, means primary node in standby cluster. */
#define SS_STREAM_MAIN_STANDBY_NODE \
        (SS_STREAM_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE))

/* Standby node in SS disaster, means standby node in standby cluster. */
#define SS_STREAM_STANDBY_CLUSTER_STANDBY_NODE \
        (SS_STREAM_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* All Standby in SS disaster, means nodes other than primary node in primary cluster and standby cluster */
#define SS_STREAM_STANDBY_NODE \
        (SS_STREAM_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))


/* dorado replication cluster in share storage mode */
const uint32 SS_DORADO_CTL_INFO_SIZE = 512;

#define SS_DORADO_CLUSTER \
        (ENABLE_DMS && ENABLE_DSS && g_instance.attr.attr_storage.ss_enable_dorado)

/* Primary Cluster in SS replication */
#define SS_DORADO_PRIMARY_CLUSTER \
        (SS_DORADO_CLUSTER && (g_instance.dms_cxt.SSReformerControl.clusterRunMode == RUN_MODE_PRIMARY))

/* Standby Cluster in SS replication */
#define SS_DORADO_STANDBY_CLUSTER \
        (SS_DORADO_CLUSTER && (g_instance.dms_cxt.SSReformerControl.clusterRunMode == RUN_MODE_STANDBY))

/* Primary node in SS replication, means primary node in main cluster. */
#define SS_DORADO_PRIMARY_NODE \
    (SS_DORADO_PRIMARY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE))

/* Standby node in SS replication, means standby node in main cluster. */
#define SS_DORADO_PRIMARY_CLUSTER_STANDBY_NODE \
    (SS_DORADO_PRIMARY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* Main standby node in SS replication, means primary node in standby cluster. */
#define SS_DORADO_MAIN_STANDBY_NODE \
    (SS_DORADO_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE))

/* Standby node in SS replication, means standby node in standby cluster. */
#define SS_DORADO_STANDBY_CLUSTER_STANDBY_NODE \
    (SS_DORADO_STANDBY_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* All Standby in SS replication, means nodes other than primary node in primary cluster and standby cluster */
#define SS_DORADO_STANDBY_NODE \
    (SS_DORADO_CLUSTER && (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE))

/* there are some same logic between ss dorado cluster and ss stream cluster */
#define SS_DISASTER_CLUSTER (SS_DORADO_CLUSTER || SS_STREAM_CLUSTER)
#define SS_DISASTER_PRIMARY_CLUSTER (SS_DORADO_PRIMARY_CLUSTER || SS_STREAM_PRIMARY_CLUSTER)
#define SS_DISASTER_STANDBY_CLUSTER (SS_DORADO_STANDBY_CLUSTER || SS_STREAM_STANDBY_CLUSTER)
#define SS_DISASTER_PRIMARY_NODE (SS_DORADO_PRIMARY_NODE || SS_STREAM_PRIMARY_NODE)
#define SS_DISASTER_MAIN_STANDBY_NODE (SS_DORADO_MAIN_STANDBY_NODE || SS_STREAM_MAIN_STANDBY_NODE)

void SSClusterDoradoStorageInit();
void InitSSDoradoCtlInfo(ShareStorageXLogCtl *ctlInfo, uint64 sysidentifier);
void UpdateSSDoradoCtlInfoAndSync();
void ReadSSDoradoCtlInfoFile();
void CheckSSDoradoCtlInfo(XLogRecPtr localEnd);

#endif // INCLUDE_SS_DISASTER_CLUSTER_H_