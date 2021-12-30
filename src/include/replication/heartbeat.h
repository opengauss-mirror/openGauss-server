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
 * heartbeat.h
 *        Data struct to store heartbeat thread variables.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _HEARTBEAT_H
#define _HEARTBEAT_H

const int MAX_EVENTS = 512;

typedef struct channel_info {
    TimestampTz connect_time;
    TimestampTz last_reply_timestamp;
} channel_info;

/*
 * Heartbeat struct in shared memory.
 */
typedef struct heartbeat_state {
    ThreadId pid; /* this heartbeat's process id, or 0 */
    int lwpId;

    channel_info channel_array[DOUBLE_MAX_REPLNODE_NUM];

    /* Protects shared variables shown above. */
    slock_t mutex;
} heartbeat_state;

extern void heartbeat_main(void);
extern TimestampTz get_last_reply_timestamp(int replindex);
extern void InitHeartbeatTimestamp();
void heartbeat_shmem_init(void);
Size heartbeat_shmem_size(void);

#endif /* _HEARTBEAT_H */
