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
 * rto_statistic.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/rto_statistic.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef RTO_STATISTIC_H
#define RTO_STATISTIC_H

#include "gs_thread.h"
#include "knl/knl_session.h"

static const uint32 RTO_VIEW_NAME_SIZE = 32;
static const uint32 RTO_VIEW_COL_SIZE = 2;
static const uint32 MAX_WAL_SENDER = 100;
static const uint32 RTO_INFO_BUFFER_SIZE = 2048 * (1 + MAX_WAL_SENDER);
static const uint32 STANDBY_NAME_SIZE = 1024;
static const uint32 RECOVERY_RTO_VIEW_COL = 9;

typedef Datum (*GetViewDataFunc)();

typedef struct RTOStatsViewObj {
    char name[RTO_VIEW_NAME_SIZE];
    Oid data_type;
    GetViewDataFunc get_data;
} RTOStatsViewObj;

/* RTO statistics */
typedef struct RTOStandbyData {
    char id[STANDBY_NAME_SIZE];
    char source_ip[IP_LEN];
    char dest_ip[IP_LEN];
    int source_port;
    int dest_port;
    int64 current_rto;
    int target_rto;
    int64 current_sleep_time;
} RTOStandbyData;

typedef struct knl_g_rto_context {
    RTOStandbyData rto_standby_data[MAX_WAL_SENDER];
} knl_g_rto_context;

extern const RTOStatsViewObj g_rtoViewArr[RTO_VIEW_COL_SIZE];
RTOStandbyData* GetRTOStat(uint32* num);
#endif /* RTO_STATISTIC_H */