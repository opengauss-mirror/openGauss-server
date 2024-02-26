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
 * ss_xmin.cpp
 *  ss xmin related
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_xmin.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_xmin.h"
#include "storage/procarray.h"
#include "storage/ipc.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_dms.h"
#include "ddes/dms/ss_common_attr.h"
#include "knl/knl_instance.h"

extern void CalculateLocalLatestSnapshot(bool forceCalc);

uint32 SSSnapshotXminKeyHashCode(const ss_snap_xmin_key_t *key)
{
    return get_hash_value(g_instance.dms_cxt.SSXminInfo.snap_cache, key);
}

uint64 GetOldestXminInNodeTable()
{
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    uint64 min_xmin = MaxTransactionId;
    for (int i = 0; i < DMS_MAX_INSTANCES; i++) {
        if ((uint64)(1 << i) > g_instance.dms_cxt.SSXminInfo.bitmap_active_nodes) {
            break;
        }

        if (!xmin_info->node_table[i].active) {
            continue;
        }

        ss_node_xmin_item_t *item = &xmin_info->node_table[i];
        SpinLockAcquire(&item->item_lock);
        if (TransactionIdPrecedes(item->notify_oldest_xmin, min_xmin)) {
            min_xmin = item->notify_oldest_xmin;
        }
        SpinLockRelease(&item->item_lock);
    }
    return min_xmin;
}

void MaintXminInPrimary(void)
{
    if (SS_IN_REFORM) {
        pg_usleep(SS_REFORM_WAIT_TIME);
        return;
    }
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    struct HTAB* snap_cache = g_instance.dms_cxt.SSXminInfo.snap_cache;
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, snap_cache);
    ss_snap_xmin_item_t *xmin_item;
    uint64 snap_xmin = MaxTransactionId;
    TimestampTz cur_time = GetCurrentTimestamp();

    for (int i = 0; i < NUM_SS_SNAPSHOT_XMIN_CACHE_PARTITIONS; i++) {
        LWLock* partition_lock = SSSnapshotXminHashPartitionLockByIndex(i);
        LWLockAcquire(partition_lock, LW_EXCLUSIVE);
    }

    while ((xmin_item = (ss_snap_xmin_item_t*)hash_seq_search(&hash_seq)) != NULL) {
        if (TimestampDifferenceExceeds(xmin_item->timestamp, cur_time, DMS_MSG_MAX_WAIT_TIME)) {
            ss_snap_xmin_key_t key{.xmin = xmin_item->xmin};
            if (hash_search(snap_cache, &key, HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("snapshot xmin cache hash table corrupted")));
            }
            continue;
        }

        if (TransactionIdPrecedes(xmin_item->xmin, snap_xmin)) {
            snap_xmin = xmin_item->xmin;
        }
    }

    for (int i = NUM_SS_SNAPSHOT_XMIN_CACHE_PARTITIONS - 1; i >= 0; i--) {
        LWLock* partition_lock = SSSnapshotXminHashPartitionLockByIndex(i);
        LWLockRelease(partition_lock);
    }
    xmin_info->snap_oldest_xmin = snap_xmin;
    uint64 new_global_xmin = GetOldestXminInNodeTable();
    if (TransactionIdPrecedes(snap_xmin, new_global_xmin)) {
        new_global_xmin = snap_xmin;
    }

    if (new_global_xmin == MaxTransactionId) {
        return;
    }

    SpinLockAcquire(&xmin_info->global_oldest_xmin_lock);
    if (xmin_info->global_oldest_xmin_active) {
        xmin_info->global_oldest_xmin = new_global_xmin;
    }
    SpinLockRelease(&xmin_info->global_oldest_xmin_lock);
}

void MaintXminInStandby(void)
{
    if (SS_IN_REFORM) {
        pg_usleep(SS_REFORM_WAIT_TIME);
        return;
    }
    uint64 oldest_xmin = MaxTransactionId;
    GetOldestGlobalProcXmin(&oldest_xmin);
    dms_context_t dms_cxt = {0};
    InitDmsContext(&dms_cxt);
    dms_send_opengauss_oldest_xmin(&dms_cxt, oldest_xmin, SS_PRIMARY_ID);
}

bool RecordSnapshotBeforeSend(uint8 inst_id, uint64 xmin)
{
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    struct HTAB* snap_cache = xmin_info->snap_cache;
    if (snap_cache == NULL) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("snap_cache is not ready, please retry")));
        return false;
    }

    ss_snap_xmin_key_t key = {.xmin = xmin};
    uint32 key_hash = SSSnapshotXminKeyHashCode(&key);
    LWLock *partition_lock = SSSnapshotXminHashPartitionLock(key_hash);
    LWLockAcquire(partition_lock, LW_EXCLUSIVE);
    ss_snap_xmin_item_t *xmin_item = (ss_snap_xmin_item_t*)hash_search(snap_cache, &key, HASH_ENTER_NULL, NULL);
    if (xmin_item == NULL) {
        LWLockRelease(partition_lock);
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("insert snapshot into snap_cache table failed, "
            "capacity is not enough")));
        return false;
    }
    xmin_item->xmin = xmin;
    TimestampTz send_time = GetCurrentTimestamp();
    xmin_item->timestamp = send_time;
    LWLockRelease(partition_lock);
    return true;
}

uint64 SSGetGlobalOldestXmin(uint64 globalxmin)
{
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    uint64 ret_globalxmin = globalxmin;
    SpinLockAcquire(&xmin_info->global_oldest_xmin_lock);
    if (!xmin_info->global_oldest_xmin_active) {
        if (TransactionIdPrecedes(xmin_info->prev_global_oldest_xmin, globalxmin)) {
            ret_globalxmin = xmin_info->prev_global_oldest_xmin;
        }
        SpinLockRelease(&xmin_info->global_oldest_xmin_lock);
        return ret_globalxmin;
    }

    if (TransactionIdPrecedes(xmin_info->global_oldest_xmin, globalxmin)) {
        ret_globalxmin = xmin_info->global_oldest_xmin;
    }
    SpinLockRelease(&xmin_info->global_oldest_xmin_lock);
    return ret_globalxmin;
}

void SSUpdateNodeOldestXmin(uint8 inst_id, unsigned long long oldest_xmin)
{
    if (!TransactionIdIsNormal(oldest_xmin)) {
        return;
    }
    
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;

    SpinLockAcquire(&xmin_info->node_table[inst_id].item_lock);
    xmin_info->node_table[inst_id].notify_oldest_xmin = oldest_xmin;
    SpinLockRelease(&xmin_info->node_table[inst_id].item_lock);
    
    if (SS_IN_REFORM) {
        if ((reform_info->bitmap_nodes |= (1 << inst_id)) == 0) {
            return;
        }

        SpinLockAcquire(&xmin_info->bitmap_active_nodes_lock);
        xmin_info->bitmap_active_nodes |= 1 << inst_id;
        xmin_info->node_table[inst_id].active = true;
        SpinLockRelease(&xmin_info->bitmap_active_nodes_lock);

        SpinLockAcquire(&xmin_info->global_oldest_xmin_lock);
        if (TransactionIdPrecedes(oldest_xmin, xmin_info->global_oldest_xmin)) {
            xmin_info->global_oldest_xmin = oldest_xmin;
        }
        SpinLockRelease(&xmin_info->global_oldest_xmin_lock);

        SpinLockAcquire(&xmin_info->bitmap_active_nodes_lock);
        if (xmin_info->bitmap_active_nodes == reform_info->bitmap_nodes) {
            SpinLockAcquire(&xmin_info->global_oldest_xmin_lock);
            xmin_info->global_oldest_xmin_active = true;
            SpinLockRelease(&xmin_info->global_oldest_xmin_lock);
        }
        SpinLockRelease(&xmin_info->bitmap_active_nodes_lock);
    }
    return;
}

void SSSyncOldestXminWhenReform(uint8 reformer_id)
{
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;

    if (reform_info->dms_role == DMS_ROLE_REFORMER) {
        while (!xmin_info->global_oldest_xmin_active) {
            if (dms_reform_failed()) {
                break;
            }
            uint64 tmp_nodes = xmin_info->bitmap_active_nodes | (1 << SS_MY_INST_ID);
            if (tmp_nodes == reform_info->bitmap_nodes) {
                break;
            }
            pg_usleep(1000L);
        }
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        CalculateLocalLatestSnapshot(true);
        LWLockRelease(ProcArrayLock);
        SpinLockAcquire(&xmin_info->snapshot_available_lock);
        xmin_info->snapshot_available = true;
        SpinLockRelease(&xmin_info->snapshot_available_lock);
    } else {
        int ret = DMS_SUCCESS;
        do {
            if (dms_reform_failed()) {
                break;
            }
            uint64 oldest_xmin = MaxTransactionId;
            GetOldestGlobalProcXmin(&oldest_xmin);
            dms_context_t dms_cxt = {0};
            InitDmsContext(&dms_cxt);
            ret = dms_send_opengauss_oldest_xmin(&dms_cxt, oldest_xmin, reformer_id);
        } while (ret != DMS_SUCCESS);
    }
}