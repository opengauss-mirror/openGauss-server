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
 * redo_statistic_msg.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/redo_statistic_msg.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef REDO_STATISTIC_MSG_H
#define REDO_STATISTIC_MSG_H
#include "access/multi_redo_settings.h"


const static uint32 REDO_WORKER_INFO_BUFFER_SIZE = 64 * (1 + MOST_FAST_RECOVERY_LIMIT);
const static uint32 VIEW_NAME_SIZE = 32;
const static uint32 REDO_VIEW_COL_SIZE = 23;

typedef struct RedoWaitInfo {
    int64 total_duration;
    int64 counter;
} RedoWaitInfo;

typedef struct XLogRedoNumStatics {
    volatile uint64 total_num;
    volatile uint64 extra_num;
} XLogRedoNumStatics;

typedef enum RedoWaitStats {
    WAIT_READ_XLOG = 0,
    WAIT_READ_DATA,
    WAIT_WRITE_DATA,
    WAIT_PROCESS_PENDING,
    WAIT_APPLY,
    WAIT_REDO_NUM
} RedoWaitStats;

/* Redo statistics */
typedef struct RedoStatsData {
    XLogRecPtr redo_start_ptr;
    int64 redo_start_time;
    int64 redo_done_time;
    int64 curr_time;
    XLogRecPtr min_recovery_point;
    XLogRecPtr read_ptr;
    XLogRecPtr last_replayed_read_ptr;
    XLogRecPtr recovery_done_ptr;
    RedoWaitInfo wait_info[WAIT_REDO_NUM];
    uint32 speed_according_seg;
    XLogRecPtr local_max_lsn;
    uint32 worker_info_len;
    char worker_info[REDO_WORKER_INFO_BUFFER_SIZE];
} RedoStatsData;



#endif