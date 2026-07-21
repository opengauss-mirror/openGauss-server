/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
 * ubmem_buf.h
 * UB transaction cache buffer manager
 *
 * src/include/access/ubmem_buf.h
 * ---------------------------------------------------------------------------------------
 */

#ifndef UBMEM_BUF_H
#define UBMEM_BUF_H

/* USE_UB_TXN_CACHE - BEGIN */
#include <atomic>
#include "c.h"

#define UB_MAX_HOST_NAME_LENGTH 255
#define UB_MAX_SHM_NAME_LENGTH 256
#define UB_MAX_REGION_NAME_DESC_LENGTH 256

typedef struct {
    std::atomic<uint64> total_size;
    std::atomic<uint64> clog_offset;
    std::atomic<uint64> clog_size;
    std::atomic<bool> clog_inited;
    std::atomic<uint64> csnlog_offset;
    std::atomic<uint64> csnlog_size;
    std::atomic<bool> csnlog_inited;
    std::atomic<uint64> oldest_xmin_offset;
    std::atomic<uint64> oldest_xmin_size;
    std::atomic<bool> oldest_xmin_inited;
    std::atomic<uint64> snapshot_offset;
    std::atomic<uint64> snapshot_size;
    std::atomic<bool> snapshot_inited;
} UBShmControlBlock;

typedef struct {
    size_t clog_size;
    size_t csnlog_size;
    size_t xmin_size;
    size_t snapshot_size;
    size_t total_size;
} UBShmStdSize;

extern void UBMemRegionName(const char *host_name, char *region_name, size_t len);
extern bool UBMemRegionInit(void);
extern bool UBSMemAllocate(const char *buffer_name, size_t buffer_size);
extern bool UBSMemLogBufferCreate(void);
extern bool UBSMemFinalize(void);
extern bool UBSMemVerification(char *ub_txn_cache_ptr);
extern bool UBSMemSyncFromOldPrimary(int32 old_primary_id, int32 new_primary_id);
extern bool UBTxnCacheAttachPrimary(void);
extern void UBTxnCacheResetReformMeta(void);

/* USE_UB_TXN_CACHE - END */
#endif /* UBMEM_BUF_H */
