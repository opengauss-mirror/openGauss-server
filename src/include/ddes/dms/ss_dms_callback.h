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
 * ss_dms_callback.h
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_dms_callback.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SS_DMS_CALLBACK_H
#define SS_DMS_CALLBACK_H

#include "ss_common_attr.h"
#include "storage/lock/s_lock.h"

/* 5 seconds */
#define REFORM_CONFIRM_TIMEOUT  5000000
#define REFORM_CONFIRM_INTERVAL 5000
#define GET_US(tv) ((int64)tv.tv_sec * 1000000U + tv.tv_usec)
#define DMS_LOGGER_BUFFER_SIZE 2048
#define REFORM_START_CLEAN_TICKS 100

typedef struct st_ss_fake_seesion_context {
    slock_t lock;
    bool *fake_sessions;
    uint32 fake_session_cnt;
    uint32 quickFetchIndex;
    uint32 session_start;
} ss_fake_seesion_context_t;

extern void DmsInitCallback(dms_callback_t *callback);
extern void DmsCallbackThreadShmemInit(unsigned char need_startup, char **reg_data);
void DmsThreadDeinit();

#endif
