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
 * ss_xmin.h
 *  include header file for ss xmin 
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_xmin.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __SS_XMIN_H__
#define __SS_XMIN_H__

#include "postgres.h"
#include "storage/lock/s_lock.h"
#include "datatype/timestamp.h"
#include "ddes/dms/dms_api.h"

typedef struct st_snap_xmin_key {
    uint64 xmin;
} ss_snap_xmin_key_t;

typedef struct st_ss_snap_xmin_item {
    uint64 xmin;
    TimestampTz timestamp;
} ss_snap_xmin_item_t;

typedef struct st_ss_node_xmin_item {
    slock_t item_lock;
    uint64 notify_oldest_xmin;
    bool active;
} ss_node_xmin_item_t;

typedef struct st_ss_xmin_info {
    ss_node_xmin_item_t node_table[DMS_MAX_INSTANCES];
    struct HTAB* snap_cache;
    uint64 snap_oldest_xmin;
    slock_t global_oldest_xmin_lock;
    uint64 global_oldest_xmin;
    uint64 prev_global_oldest_xmin;
    bool global_oldest_xmin_active;
    slock_t bitmap_active_nodes_lock;
    uint64 bitmap_active_nodes;
    slock_t snapshot_available_lock;
    bool snapshot_available;
} ss_xmin_info_t;

#define SSSnapshotXminHashPartition(hashcode) ((hashcode) % NUM_SS_SNAPSHOT_XMIN_CACHE_PARTITIONS)
#define SSSnapshotXminHashPartitionLock(hashcode) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstSSSnapshotXminCacheLock + SSSnapshotXminHashPartition(hashcode)].lock)
#define SSSnapshotXminHashPartitionLockByIndex(i) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstSSSnapshotXminCacheLock + (i)].lock)
uint32 SSSnapshotXminKeyHashCode(const ss_snap_xmin_key_t *key);
void MaintXminInPrimary(void);
void MaintXminInStandby(void);
bool RecordSnapshotBeforeSend(uint8 inst_id, uint64 xmin);
uint64 SSGetGlobalOldestXmin(uint64 globalxmin);
void SSUpdateNodeOldestXmin(uint8 inst_id, unsigned long long oldest_xmin);
void SSSyncOldestXminWhenReform(uint8 reformer_id);

#endif