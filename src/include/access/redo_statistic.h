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
 * redo_statistic.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/redo_statistic.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef REDO_STATISTIC_H
#define REDO_STATISTIC_H

#include "gs_thread.h"
#include "knl/knl_instance.h"
#include "pgstat.h"
#include "access/redo_statistic_msg.h"

typedef Datum (*GetViewDataFunc)();

typedef struct RedoStatsViewObj {
    char name[VIEW_NAME_SIZE];
    Oid data_type;
    GetViewDataFunc get_data;
} RedoStatsViewObj;

typedef enum RedoWorkerWaitSyncStats {
    WAIT_SYNC_AP_REF = 0, /* ApplyMultiPageRecord refCount*/
    WAIT_SYNC_AP_REP,     /* ApplyMultiPageRecord replayed*/
    WAIT_SYNC_AMPSH,      /* ApplyMultiPageShareWithTrxnRecord */
    WAIT_SYNC_AMPSY,      /* ApplyMultiPageSyncWithTrxnRecord */
    WAIT_SYNC_ARAS,       /* ApplyReadyAllShareLogRecords */
    WAIT_SYNC_ARTS,       /* ApplyReadyTxnShareLogRecords */
    WAIT_SYNC_GXR,        /* GetXlogReader */
    WAIT_SYNC_NUM
} RedoWorkerWaitSyncStats;

/* Redo statistics */
typedef struct RedoWorkerStatsData {
    uint32 id;              /* Worker id. */
    uint32 queue_usage;     /* queue usage */
    uint32 queue_max_usage; /* the max usage of queue */
    /* XLogRecPtr head_ptr; do not try to get head_ptr and tail_ptr, */
    /* XLogRecPtr tail_ptr; because the memory of redoItem maybe be freed already */
    uint64 redo_rec_count;
} RedoWorkerStatsData;

extern const RedoStatsViewObj g_redoViewArr[REDO_VIEW_COL_SIZE];

extern void redo_fill_redo_event();
extern void redo_refresh_stats(uint64 speed);
extern void redo_unlink_stats_file();

static const uint64 US_TRANSFER_TO_S = (1000000);
static const uint64 BYTES_TRANSFER_KBYTES = (1024);
static const uint64 MAX_OUT_INTERVAL = (30 * 1000000);


WaitEventIO redo_get_event_type_by_wait_type(uint32 type);
char* redo_get_name_by_wait_type(uint32 type);


#endif /* DOUBLE_WRITE_H */
