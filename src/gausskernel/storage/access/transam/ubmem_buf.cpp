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
 * ubmem_buf.cpp
 * UB transaction cache buffer manager
 *
 * src/gausskernel/storage/access/transam/ubmem_buf.cpp
 * ---------------------------------------------------------------------------------------
 */

/* USE_UB_TXN_CACHE - BEGIN */
#include <string.h>
#include <pwd.h>
#include <unistd.h>
#include "knl/knl_thread.h"
#include "access/clog.h"
#include "access/csnlog.h"
#include "ddes/dms/ss_init.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_xmin.h"
#include "storage/ubs_mem.h"
#include "utils/elog.h"
#include "access/ub_sigbus_handler.h"
#include "securec_check.h"
#include "access/ubmem_buf.h"

namespace {
struct UBLocalMapRecord {
    std::atomic<uintptr_t> addr;
    std::atomic<uint64> size;
};
}  // namespace

static inline UBLocalMapRecord *UBGetLocalMapRecords(void)
{
    return (UBLocalMapRecord *)g_instance.shmem_cxt.UBLocalMapRecordsPtr;
}

/* Allocate the per-process remembered UB mapping table during UB init. */
static bool UBInitLocalMapRecords(void)
{
    if (UBGetLocalMapRecords() != nullptr || !ENABLE_UB) {
        return true;
    }

    /*
     * ProcessMemory itself is sealed after startup, so allocate this small
     * per-process helper table from the default instance memory context.
     */
    UBLocalMapRecord *records = (UBLocalMapRecord *)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT), sizeof(UBLocalMapRecord) * DMS_MAX_INSTANCE);
    if (records == nullptr) {
        ereport(ERROR, (errmsg("Failed to allocate UB local map records")));
        return false;
    }

    g_instance.shmem_cxt.UBLocalMapRecordsPtr = records;
    return true;
}

/* Release the remembered UB mapping table when the UB runtime is finalized. */
static void UBReleaseLocalMapRecords(void)
{
    UBLocalMapRecord *records = UBGetLocalMapRecords();
    if (records != nullptr) {
        pfree(records);
        g_instance.shmem_cxt.UBLocalMapRecordsPtr = nullptr;
    }
}

/* Remember one successfully mapped UB shm address for later reuse on remap failures. */
static void UBRememberLocalMapping(int32 instance_id, void *addr, size_t size)
{
    if (instance_id < 0 || instance_id >= DMS_MAX_INSTANCE || addr == nullptr) {
        return;
    }

    UBLocalMapRecord *records = UBGetLocalMapRecords();
    if (records == nullptr) {
        return;
    }
    records[instance_id].size.store((uint64)size, std::memory_order_release);
    records[instance_id].addr.store((uintptr_t)addr, std::memory_order_release);
}

/* Forget a remembered UB mapping once the corresponding unmap succeeds. */
static void UBForgetLocalMappingByAddr(void *addr)
{
    UBLocalMapRecord *records = UBGetLocalMapRecords();
    if (addr == nullptr || records == nullptr) {
        return;
    }

    uintptr_t target = (uintptr_t)addr;
    for (int32 i = 0; i < DMS_MAX_INSTANCE; ++i) {
        uintptr_t saved_addr = records[i].addr.load(std::memory_order_acquire);
        if (saved_addr == target) {
            records[i].size.store(0, std::memory_order_release);
            records[i].addr.store(0, std::memory_order_release);
        }
    }
}

static bool UBUnmapLocalMapping(char *addr, size_t size, const char *desc)
{
    if (addr == nullptr || size == 0) {
        return true;
    }

    int ret = ubsmem_shmem_unmap(addr, size);
    if (ret != UBSM_OK) {
        ereport(WARNING, (errmsg("Failed to unmap %s UB shared memory: %d, addr=%p, size=%lu",
                                 desc, ret, addr, (unsigned long)size)));
        return false;
    }

    UBForgetLocalMappingByAddr(addr);
    return true;
}

/* Reuse a still-valid remembered mapping when the SDK rejects a duplicate map. */
static bool UBTryReuseRememberedMapping(int32 instance_id, size_t expected_size, char **reuse_addr)
{
    UBLocalMapRecord *records = UBGetLocalMapRecords();
    if (reuse_addr == nullptr || instance_id < 0 || instance_id >= DMS_MAX_INSTANCE || records == nullptr) {
        return false;
    }

    uintptr_t saved_addr = records[instance_id].addr.load(std::memory_order_acquire);
    uint64 saved_size = records[instance_id].size.load(std::memory_order_acquire);
    if (saved_addr == 0) {
        return false;
    }

    char *addr = (char *)saved_addr;
    if (saved_size != 0 && (size_t)saved_size != expected_size) {
        ereport(WARNING, (errmsg("Remembered UB shared memory size mismatch: inst=%d, addr=%p, "
                                 "saved_size=%lu, expected_size=%lu",
                                 instance_id, addr, (unsigned long)saved_size, (unsigned long)expected_size)));
        (void)UBUnmapLocalMapping(addr, (size_t)saved_size, "mismatched remembered");
        return false;
    }

    bool verify_ok = false;
    int ub_fault_rc = sigsetjmp(jump_env, 1);
    if (ub_fault_rc == 0) {
        ub_sigbus_jump_active = 1;
        verify_ok = UBSMemVerification(addr);
        UB_ESB_BARRIER();
        ub_sigbus_jump_active = 0;
    } else {
        ub_sigbus_jump_active = 0;
        g_instance.shmem_cxt.UBMemAccessEnabled.store(false, std::memory_order_release);
        ereport(WARNING, (errmsg("[SIGBUS] fault captured in UBTryReuseRememberedMapping (verification)")));
        verify_ok = false;
    }
    if (!verify_ok) {
        ereport(WARNING, (errmsg("Remembered UB shared memory verification failed: inst=%d, addr=%p",
                                 instance_id, addr)));
        size_t mapSize = (saved_size != 0) ? (size_t)saved_size : expected_size;
        (void)UBUnmapLocalMapping(addr, mapSize, "invalid remembered");
        return false;
    }

    *reuse_addr = addr;
    return true;
}

static const char* GetOSUserName(char *buf, size_t len)
{
    struct passwd *pw = getpwuid(getuid());
    if (pw == nullptr || pw->pw_name == nullptr) {
        ereport(ERROR, (errmsg("Failed to get OS user name")));
        return nullptr;
    }
    errno_t rc = snprintf_s(buf, len, len - 1, "%s", pw->pw_name);
    securec_check_ss_c(rc, "\0", "\0");
    return buf;
}

static bool GetUBSMemName(int32 instance_id, char *shm_name, size_t shm_name_size)
{
    char user_name[128];
    if (GetOSUserName(user_name, sizeof(user_name)) == nullptr) {
        ereport(ERROR, (errmsg("Failed to get OS user name for instance %d", instance_id)));
        return false;
    }
    
    errno_t rc = snprintf_s(shm_name, shm_name_size, shm_name_size - 1,
                         "ub_node%02d_%s_txn_cache", instance_id, user_name);
    securec_check_ss_c(rc, "\0", "\0");
    return true;
}

void UBMemRegionName(const char *host_name, char *region_name, size_t len)
{
    if (host_name == nullptr || region_name == nullptr) {
        ereport(ERROR, (errmsg("Invalid arguments")));
        return;
    }
    errno_t rc = snprintf_s(region_name, len, len - 1, "mem_pool_%s_inst%d", host_name, SS_MY_INST_ID);
    securec_check_ss_c(rc, "\0", "\0");
}

bool UBMemRegionInit()
{
    char host_name[UB_MAX_HOST_NAME_LENGTH + 1];
    if (gethostname(host_name, sizeof(host_name)) != 0) {
        ereport(ERROR, (errmsg("Failed to get host name")));
        return false;
    }
    host_name[sizeof(host_name) - 1] = '\0';

    ubsmem_regions_t regions;
    ubsmem_region_attributes_t region;
    int ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        ereport(ERROR, (errmsg("Failed to lookup shm regions, error: %d", ret)));
        return false;
    }

    region = regions.region[0];
    for (int i = 0; i < region.host_num; i++) {
        region.hosts[i].affinity = false;
        if (strcmp(region.hosts[i].host_name, host_name) == 0) {
            region.hosts[i].affinity = true;
        }
    }

    char region_name[UB_MAX_REGION_NAME_DESC_LENGTH];
    UBMemRegionName(host_name, region_name, sizeof(region_name));

    ret = ubsmem_create_region(region_name, 0, &region);
    if (ret != UBSM_OK && ret != UBSM_ERR_ALREADY_EXIST) {
        ereport(ERROR, (errmsg("Failed to create region. ret: %d", ret)));
        return false;
    }
    return true;
}

static void InitUBSMemControlBlock(UBShmControlBlock *ctrl, size_t total_size, size_t clog_size,
                                   size_t csnlog_size, size_t xmin_size, size_t snapshot_size)
{
    ctrl->total_size.store(total_size, std::memory_order_release);
    ctrl->clog_offset.store(sizeof(UBShmControlBlock), std::memory_order_release);
    ctrl->clog_size.store(clog_size, std::memory_order_release);
    ctrl->clog_inited.store(false, std::memory_order_release);

    uint64 csnlog_off = sizeof(UBShmControlBlock) + clog_size;
    ctrl->csnlog_offset.store(csnlog_off, std::memory_order_release);
    ctrl->csnlog_size.store(csnlog_size, std::memory_order_release);
    ctrl->csnlog_inited.store(false, std::memory_order_release);

    uint64 oldest_xmin_off = csnlog_off + csnlog_size;
    ctrl->oldest_xmin_offset.store(oldest_xmin_off, std::memory_order_release);
    ctrl->oldest_xmin_size.store(xmin_size, std::memory_order_release);
    ctrl->oldest_xmin_inited.store(false, std::memory_order_release);

    uint64 snapshot_off = oldest_xmin_off + xmin_size;
    ctrl->snapshot_offset.store(snapshot_off, std::memory_order_release);
    ctrl->snapshot_size.store(snapshot_size, std::memory_order_release);
    ctrl->snapshot_inited.store(false, std::memory_order_release);
}

static void InitUBSMemBuffer(UBShmControlBlock *ctrl, char *base_addr)
{
    bool clog_inited = false;
    bool csnlog_inited = false;
    bool oldest_xmin_inited = false;
    bool snapshot_inited = false;
    uint64 clog_off = 0;
    uint64 csnlog_off = 0;
    uint64 oldest_xmin_off = 0;
    uint64 snapshot_off = 0;

    int ub_fault_rc = sigsetjmp(jump_env, 1);
    if (ub_fault_rc == 0) {
        ub_sigbus_jump_active = 1;
        clog_inited = ctrl->clog_inited.load(std::memory_order_acquire);
        clog_off = ctrl->clog_offset.load();
        csnlog_inited = ctrl->csnlog_inited.load(std::memory_order_acquire);
        csnlog_off = ctrl->csnlog_offset.load();
        oldest_xmin_inited = ctrl->oldest_xmin_inited.load(std::memory_order_acquire);
        oldest_xmin_off = ctrl->oldest_xmin_offset.load();
        snapshot_inited = ctrl->snapshot_inited.load(std::memory_order_acquire);
        snapshot_off = ctrl->snapshot_offset.load();
        UB_ESB_BARRIER();
        ub_sigbus_jump_active = 0;
    } else {
        ub_sigbus_jump_active = 0;
        g_instance.shmem_cxt.UBMemAccessEnabled.store(false, std::memory_order_release);
        ereport(WARNING, (errmsg("[SIGBUS] fault captured in InitUBSMemBuffer (offset read)")));
        return;
    }

    if (!clog_inited) {
        UBCLogBuffer *clog_buf = (UBCLogBuffer *)(base_addr + clog_off);
        UBCLogBufferInit(clog_buf);
        if (!g_instance.shmem_cxt.UBMemAccessEnabled.load(std::memory_order_acquire)) {
            return;
        }
        ctrl->clog_inited.store(true, std::memory_order_release);
    }
    if (!csnlog_inited) {
        UBCSNLogBuffer *csnlog_buf = (UBCSNLogBuffer *)(base_addr + csnlog_off);
        UBCSNLogBufferInit(csnlog_buf);
        if (!g_instance.shmem_cxt.UBMemAccessEnabled.load(std::memory_order_acquire)) {
            return;
        }
        ctrl->csnlog_inited.store(true, std::memory_order_release);
    }
    if (!oldest_xmin_inited) {
        UBOldestXminBuffer *xmin_buf = (UBOldestXminBuffer *)(base_addr + oldest_xmin_off);
        UBOldestXminBufferInit(xmin_buf);
        if (!g_instance.shmem_cxt.UBMemAccessEnabled.load(std::memory_order_acquire)) {
            return;
        }
        ctrl->oldest_xmin_inited.store(true, std::memory_order_release);
    }
    if (!snapshot_inited) {
        UBSnapshotBuffer *snapshot_buf = (UBSnapshotBuffer *)(base_addr + snapshot_off);
        UBSnapshotBufferInit(snapshot_buf);
        if (!g_instance.shmem_cxt.UBMemAccessEnabled.load(std::memory_order_acquire)) {
            return;
        }
        ctrl->snapshot_inited.store(true, std::memory_order_release);
    }
}

static UBShmStdSize CalUBSMemSize(void)
{
    const size_t MIN_SIZE = 128 << 20;
    const size_t ALIGN_SIZE = 4 << 20;
    const size_t GB_ALIGN = 1ULL << 30;

    size_t clog_buf_size = UBCLogBufferSize();
    size_t csnlog_buf_size = UBCSNLogBufferSize();
    size_t xmin_buf_size = UBOldestXminBufferSize();
    size_t snapshot_buf_size = UBSnapshotBufferSize();

    UBShmStdSize sz;
    sz.clog_size = (clog_buf_size < MIN_SIZE) ? MIN_SIZE :
                   ((clog_buf_size + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE);
    sz.csnlog_size = (csnlog_buf_size < MIN_SIZE) ? MIN_SIZE :
                     ((csnlog_buf_size + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE);
    sz.xmin_size = (xmin_buf_size < MIN_SIZE) ? MIN_SIZE :
                   ((xmin_buf_size + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE);
    sz.snapshot_size = (snapshot_buf_size < MIN_SIZE) ? MIN_SIZE :
                       ((snapshot_buf_size + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE);

    sz.total_size = sizeof(UBShmControlBlock) + sz.clog_size + sz.csnlog_size + sz.xmin_size + sz.snapshot_size;
    sz.total_size = (sz.total_size + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE;
    sz.total_size = (sz.total_size + GB_ALIGN - 1) / GB_ALIGN * GB_ALIGN;

    return sz;
}

static void UBClearTxnCachePointers(void)
{
    g_instance.shmem_cxt.UBTxnCachePtr = nullptr;
    g_instance.shmem_cxt.UBClogBufPtr = nullptr;
    g_instance.shmem_cxt.UBCSNLogBufPtr = nullptr;
    g_instance.shmem_cxt.UBOldestXminBufPtr = nullptr;
    g_instance.shmem_cxt.UBSnapshotBufPtr = nullptr;
}

static void UBRefreshTxnCachePointers(void)
{
    UBCLogShmemInit();
    UBCSNLogShmemInit();
    UBOldestXminShmemInit();
    UBSnapshotShmemInit();
}

bool UBSMemAllocate(const char *buffer_name, size_t buffer_size)
{
    char host_name[UB_MAX_HOST_NAME_LENGTH + 1];
    if (gethostname(host_name, sizeof(host_name)) != 0) {
        ereport(ERROR, (errmsg("Failed to get host name")));
        return false;
    }
    host_name[sizeof(host_name) - 1] = '\0';

    char region_name[UB_MAX_REGION_NAME_DESC_LENGTH];
    UBMemRegionName(host_name, region_name, sizeof(region_name));

    int ret = ubsmem_shmem_allocate(region_name, buffer_name, buffer_size, 0600,
                                    UBSM_FLAG_ONLY_IMPORT_NONCACHE | UBSM_FLAG_WR_DELAY_COMP);
    if (ret != UBSM_OK && ret != UBSM_ERR_ALREADY_EXIST) {
        ereport(ERROR, (errmsg("Failed to allocate UB shared memory for %s: %d", buffer_name, ret)));
        return false;
    }

    ret = ubsmem_destroy_region(region_name);
    if (ret != UBSM_OK) {
        ereport(ERROR, (errmsg("Failed to destroy region. ret: %d", ret)));
        return false;
    }
    
    return true;
}

bool UBSMemLogBufferCreate()
{
    UBShmStdSize sz = CalUBSMemSize();

    char user_name[128];
    if (GetOSUserName(user_name, sizeof(user_name)) == nullptr) {
        return false;
    }

    char local_shm_name[UB_MAX_SHM_NAME_LENGTH];
    char primary_shm_name[UB_MAX_SHM_NAME_LENGTH];
    errno_t rc = snprintf_s(local_shm_name, sizeof(local_shm_name), sizeof(local_shm_name) - 1,
                        "ub_node%02d_%s_txn_cache", SS_MY_INST_ID, user_name);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(primary_shm_name, sizeof(primary_shm_name), sizeof(primary_shm_name) - 1,
                        "ub_node%02d_%s_txn_cache", SS_PRIMARY_ID, user_name);
    securec_check_ss_c(rc, "\0", "\0");

    if (!UBInitLocalMapRecords()) {
        return false;
    }

    if (!UBSMemAllocate(local_shm_name, sz.total_size)) {
        return false;
    }

    void *local_addr = nullptr;
    int ret = ubsmem_shmem_map(nullptr, sz.total_size, PROT_READ | PROT_WRITE,
                               MAP_SHARED, local_shm_name, 0, &local_addr);
    if (ret != UBSM_OK || local_addr == nullptr) {
        ereport(ERROR, (errmsg("Failed to map UB shared memory: %d, local_shm_name=%s", ret, local_shm_name)));
        return false;
    }
    UBRememberLocalMapping(SS_MY_INST_ID, local_addr, sz.total_size);

    UBShmControlBlock *ctrl = (UBShmControlBlock *)local_addr;
    bool new_created = (ctrl->total_size.load(std::memory_order_relaxed) == 0);
    if (new_created) {
        InitUBSMemControlBlock(ctrl, sz.total_size, sz.clog_size, sz.csnlog_size, sz.xmin_size, sz.snapshot_size);
    } else {
        if (!UBSMemVerification((char *)local_addr)) {
            ereport(LOG, (errmsg("UB shared memory verification failed, reinitializing control block")));
            InitUBSMemControlBlock(ctrl, sz.total_size, sz.clog_size, sz.csnlog_size, sz.xmin_size, sz.snapshot_size);
        }
    }

    InitUBSMemBuffer(ctrl, (char *)local_addr);

    (void)UBUnmapLocalMapping((char *)local_addr, sz.total_size, "local");

    void *addr = nullptr;
    ret = ubsmem_shmem_map(nullptr, sz.total_size, PROT_READ | PROT_WRITE,
                           MAP_SHARED, primary_shm_name, 0, &addr);
    if (ret != UBSM_OK || addr == nullptr) {
        ereport(ERROR, (errmsg("Failed to map UB shared memory: %d, primary_shm_name=%s", ret, primary_shm_name)));
        return false;
    }
    UBRememberLocalMapping(SS_PRIMARY_ID, addr, sz.total_size);

    g_instance.shmem_cxt.UBTxnCachePtr = (char *)addr;

    /* Print partition addresses for fault injection experiments */
    UBShmControlBlock *primary_ctrl = (UBShmControlBlock *)addr;
    char *base_addr = (char *)addr;
    uint64 clog_off = primary_ctrl->clog_offset.load(std::memory_order_acquire);
    uint64 csnlog_off = primary_ctrl->csnlog_offset.load(std::memory_order_acquire);
    uint64 xmin_off = primary_ctrl->oldest_xmin_offset.load(std::memory_order_acquire);
    uint64 snap_off = primary_ctrl->snapshot_offset.load(std::memory_order_acquire);
    ereport(LOG, (errmsg("UB shared memory initialized (size: %lu, %s). "
                         "Partition addresses for fault injection: "
                         "base=%p, clog=%p (off=%lu, size=%lu), "
                         "csnlog=%p (off=%lu, size=%lu), "
                         "oldest_xmin=%p (off=%lu, size=%lu), "
                         "snapshot=%p (off=%lu, size=%lu)",
                         sz.total_size, new_created ? "new create" : "reused",
                         base_addr,
                         base_addr + clog_off, clog_off, sz.clog_size,
                         base_addr + csnlog_off, csnlog_off, sz.csnlog_size,
                         base_addr + xmin_off, xmin_off, sz.xmin_size,
                         base_addr + snap_off, snap_off, sz.snapshot_size)));
    return true;
}

bool UBSMemFinalize()
{
    char *addr = g_instance.shmem_cxt.UBTxnCachePtr;
    if (addr != nullptr) {
        UBShmControlBlock *ctrl = (UBShmControlBlock *)addr;
        size_t length = (size_t)ctrl->total_size.load(std::memory_order_acquire);
        (void)UBUnmapLocalMapping(addr, length, "current");
        UBClearTxnCachePointers();
    }

    UBLocalMapRecord *records = UBGetLocalMapRecords();
    if (records != nullptr) {
        for (int32 i = 0; i < DMS_MAX_INSTANCE; ++i) {
            uintptr_t saved_addr = records[i].addr.load(std::memory_order_acquire);
            uint64 saved_size = records[i].size.load(std::memory_order_acquire);
            if (saved_addr == 0 || saved_size == 0) {
                continue;
            }

            (void)UBUnmapLocalMapping((char *)saved_addr, (size_t)saved_size, "remembered");
        }
    }

    UBReleaseLocalMapRecords();
    return true;
}

bool UBSMemVerification(char *ub_txn_cache_ptr)
{
    if (ub_txn_cache_ptr == nullptr) {
        ereport(WARNING, (errmsg("UB shared memory pointer is null")));
        return false;
    }

    UBShmControlBlock *ctrl = (UBShmControlBlock *)ub_txn_cache_ptr;

    uint64 total_size = ctrl->total_size.load(std::memory_order_acquire);
    uint64 clog_offset = ctrl->clog_offset.load(std::memory_order_acquire);
    uint64 clog_size = ctrl->clog_size.load(std::memory_order_acquire);
    uint64 csnlog_offset = ctrl->csnlog_offset.load(std::memory_order_acquire);
    uint64 csnlog_size = ctrl->csnlog_size.load(std::memory_order_acquire);
    uint64 oldest_xmin_offset = ctrl->oldest_xmin_offset.load(std::memory_order_acquire);
    uint64 oldest_xmin_size = ctrl->oldest_xmin_size.load(std::memory_order_acquire);
    uint64 snapshot_offset = ctrl->snapshot_offset.load(std::memory_order_acquire);
    uint64 snapshot_size = ctrl->snapshot_size.load(std::memory_order_acquire);

    bool valid = true;

    uint64 expected_clog_offset = sizeof(UBShmControlBlock);
    if (clog_offset != expected_clog_offset) {
        ereport(WARNING, (errmsg("UB memory verification failed: clog_offset mismatch. "
                                 "expected: %lu, actual: %lu", expected_clog_offset, clog_offset)));
        valid = false;
    }

    uint64 expected_csnlog_offset = clog_offset + clog_size;
    if (csnlog_offset != expected_csnlog_offset) {
        ereport(WARNING, (errmsg("UB memory verification failed: csnlog_offset mismatch. "
                                 "expected: %lu (clog_offset %lu + clog_size %lu), actual: %lu",
                                 expected_csnlog_offset, clog_offset, clog_size, csnlog_offset)));
        valid = false;
    }

    uint64 expected_oldest_xmin_offset = csnlog_offset + csnlog_size;
    if (oldest_xmin_offset != expected_oldest_xmin_offset) {
        ereport(WARNING, (errmsg("UB memory verification failed: oldest_xmin_offset mismatch. "
                                 "expected: %lu (csnlog_offset %lu + csnlog_size %lu), actual: %lu",
                                 expected_oldest_xmin_offset, csnlog_offset, csnlog_size, oldest_xmin_offset)));
        valid = false;
    }

    uint64 expected_snapshot_offset = oldest_xmin_offset + oldest_xmin_size;
    if (snapshot_offset != expected_snapshot_offset) {
        ereport(WARNING, (errmsg("UB memory verification failed: snapshot_offset mismatch. "
                                 "expected: %lu (oldest_xmin_offset %lu + oldest_xmin_size %lu), actual: %lu",
                                 expected_snapshot_offset, oldest_xmin_offset, oldest_xmin_size, snapshot_offset)));
        valid = false;
    }

    if (valid) {
        ereport(LOG, (errmsg("UB memory verification passed. "
                             "total_size: %lu, clog: [%lu, %lu], csnlog: [%lu, %lu], "
                             "oldest_xmin: [%lu, %lu], snapshot: [%lu, %lu]",
                             total_size, clog_offset, clog_size, csnlog_offset, csnlog_size,
                             oldest_xmin_offset, oldest_xmin_size, snapshot_offset, snapshot_size)));
    }

    return valid;
}

/*
 * Re-attach this process to the current primary UB txn cache.
 *
 * This is used by standby/reform flows after the primary role changes. We only
 * need to remap the primary's existing UB shared memory and refresh the local
 * cached pointers; the shared memory contents are owned by the primary side.
 */
bool UBTxnCacheAttachPrimary(void)
{
    UBShmStdSize sz = CalUBSMemSize();
    size_t new_map_size = sz.total_size;
    char primary_shm_name[UB_MAX_SHM_NAME_LENGTH];
    if (!GetUBSMemName(SS_PRIMARY_ID, primary_shm_name, sizeof(primary_shm_name))) {
        return false;
    }

    void *old_addr = (void *)g_instance.shmem_cxt.UBTxnCachePtr;
    size_t old_map_size = new_map_size;
    if (old_addr != nullptr) {
        UBShmControlBlock *old_ctrl = (UBShmControlBlock *)old_addr;
        if (old_ctrl->total_size.load(std::memory_order_acquire) != 0) {
            old_map_size = (size_t)old_ctrl->total_size.load(std::memory_order_acquire);
        }
    }

    void *new_addr = nullptr;
    int ret = ubsmem_shmem_map(nullptr, new_map_size, PROT_READ | PROT_WRITE,
                               MAP_SHARED, primary_shm_name, 0, &new_addr);
    if (ret != UBSM_OK || new_addr == nullptr) {
        /*
         * If the previous unmap failed, the SDK may reject a second map of the
         * same primary UB shm in this process. Reuse the remembered mapping
         * after a light validation instead of failing the reform path.
         */
        if (ret == UBSM_ERR_PARAM_INVALID && UBTryReuseRememberedMapping(SS_PRIMARY_ID, new_map_size,
            (char **)&new_addr)) {
            ereport(WARNING, (errmsg("Current primary UB shared memory is already mapped locally, "
                                     "reusing remembered mapping: shm_name=%s, addr=%p",
                                     primary_shm_name, new_addr)));
        } else {
            ereport(ERROR, (errmsg("Failed to map current primary UB shared memory: %d, shm_name=%s",
                                   ret, primary_shm_name)));
            return false;
        }
    }
    UBRememberLocalMapping(SS_PRIMARY_ID, new_addr, new_map_size);

    bool verify_ok = false;
    int ub_fault_rc_v = sigsetjmp(jump_env, 1);
    if (ub_fault_rc_v == 0) {
        ub_sigbus_jump_active = 1;
        verify_ok = UBSMemVerification((char *)new_addr);
        UB_ESB_BARRIER();
        ub_sigbus_jump_active = 0;
    } else {
        ub_sigbus_jump_active = 0;
        g_instance.shmem_cxt.UBMemAccessEnabled.store(false, std::memory_order_release);
        ereport(WARNING, (errmsg("[SIGBUS] fault captured in UBTxnCacheAttachPrimary (verification)")));
        verify_ok = false;
    }
    if (!verify_ok) {
        (void)UBUnmapLocalMapping((char *)new_addr, new_map_size, "invalid current primary");
        ereport(ERROR, (errmsg("Current primary UB shared memory verification failed, shm_name=%s",
                               primary_shm_name)));
        return false;
    }

    /*
     * Publish the new base and derived UB pointers before unmapping the old
     * primary shm, so concurrent UB users stop touching the old mapping first.
     */
    g_instance.shmem_cxt.UBTxnCachePtr = (char *)new_addr;
    UBRefreshTxnCachePointers();
    if (old_addr != nullptr && old_addr != new_addr) {
        (void)UBUnmapLocalMapping((char *)old_addr, old_map_size, "old primary after remap");
    }
    return true;
}

void UBTxnCacheResetReformMeta(void)
{
    char *base_addr = g_instance.shmem_cxt.UBTxnCachePtr;
    if (base_addr == nullptr) {
        return;
    }

    UBShmControlBlock *ctrl = (UBShmControlBlock *)base_addr;
    UBOldestXminBuffer *xmin_buf = nullptr;
    UBSnapshotBuffer *snapshot_buf = nullptr;

    int ub_fault_rc = sigsetjmp(jump_env, 1);
    if (ub_fault_rc == 0) {
        ub_sigbus_jump_active = 1;
        xmin_buf = (UBOldestXminBuffer *)(base_addr + ctrl->oldest_xmin_offset.load());
        snapshot_buf = (UBSnapshotBuffer *)(base_addr + ctrl->snapshot_offset.load());
        UB_ESB_BARRIER();
        ub_sigbus_jump_active = 0;
    } else {
        ub_sigbus_jump_active = 0;
        g_instance.shmem_cxt.UBMemAccessEnabled.store(false, std::memory_order_release);
        ereport(WARNING, (errmsg("[SIGBUS] fault captured in UBTxnCacheResetReformMeta (offset read)")));
        return;
    }

    UBOldestXminBufferInit(xmin_buf);
    ctrl->oldest_xmin_inited.store(true, std::memory_order_release);
    UBSnapshotBufferInit(snapshot_buf);
    ctrl->snapshot_inited.store(true, std::memory_order_release);
}

static bool UBSBufferSafeMemcpy(char *dest, const char *src, size_t size)
{
    const size_t CHUNK_SIZE = 1024 * 1024 * 1024;
    size_t offset = 0;
    
    while (offset < size) {
        size_t chunk = (size - offset) < CHUNK_SIZE ? (size - offset) : CHUNK_SIZE;
        errno_t rc = memcpy_s(dest + offset, chunk, src + offset, chunk);
        securec_check_c(rc, "\0", "\0");
        offset += chunk;
    }
    return true;
}

bool UBSMemSyncFromOldPrimary(int32 old_primary_id, int32 new_primary_id)
{
    if (old_primary_id < 0 || old_primary_id >= DMS_MAX_INSTANCE) {
        ereport(ERROR, (errmsg("Invalid old primary ID: %d", old_primary_id)));
        return false;
    }
    
    if (new_primary_id < 0 || new_primary_id >= DMS_MAX_INSTANCE) {
        ereport(ERROR, (errmsg("Invalid new primary ID: %d", new_primary_id)));
        return false;
    }
    
    if (old_primary_id == new_primary_id) {
        ereport(ERROR, (errmsg("Old primary ID equals new primary ID: %d", old_primary_id)));
        return false;
    }
    
    char old_shm_name[UB_MAX_SHM_NAME_LENGTH];
    char new_shm_name[UB_MAX_SHM_NAME_LENGTH];
    
    if (!GetUBSMemName(old_primary_id, old_shm_name, sizeof(old_shm_name))) {
        return false;
    }
    
    if (!GetUBSMemName(new_primary_id, new_shm_name, sizeof(new_shm_name))) {
        return false;
    }
    
    UBShmStdSize sz = CalUBSMemSize();
    char *old_primary_addr = nullptr;
    char *new_primary_addr = nullptr;
    size_t total_size = sz.total_size;
    
    if (g_instance.shmem_cxt.UBTxnCachePtr != nullptr) {
        old_primary_addr = g_instance.shmem_cxt.UBTxnCachePtr;
    } else {
        int map_ret = ubsmem_shmem_map(nullptr, total_size, PROT_READ | PROT_WRITE,
                                        MAP_SHARED, old_shm_name, 0, (void **)&old_primary_addr);
        if (map_ret != UBSM_OK || old_primary_addr == nullptr) {
            ereport(ERROR, (errmsg("Failed to map old primary UB shared memory: %d", map_ret)));
            return false;
        }
    }
    UBShmControlBlock *old_ctrl = (UBShmControlBlock *)old_primary_addr;
    total_size = (size_t)old_ctrl->total_size.load(std::memory_order_acquire);
    UBRememberLocalMapping(old_primary_id, old_primary_addr, total_size);

    int map_ret = ubsmem_shmem_map(nullptr, total_size, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, new_shm_name, 0, (void **)&new_primary_addr);
    if (map_ret == UBSM_ERR_NOT_FOUND) {
        if (!UBMemRegionInit()) {
            ereport(ERROR, (errmsg("Failed to initialize UB memory region")));
            return false;
        }
        
        if (!UBSMemAllocate(new_shm_name, total_size)) {
            ereport(ERROR, (errmsg("Failed to allocate UB shared memory for new primary")));
            return false;
        }
        
        map_ret = ubsmem_shmem_map(nullptr, total_size, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, new_shm_name, 0, (void **)&new_primary_addr);
        if (map_ret != UBSM_OK || new_primary_addr == nullptr) {
            ereport(ERROR, (errmsg("Failed to map new primary UB shared memory after creation: %d", map_ret)));
            return false;
        }
    } else if (map_ret != UBSM_OK || new_primary_addr == nullptr) {
        ereport(ERROR, (errmsg("Failed to map new primary UB shared memory: %d", map_ret)));
        return false;
    }
    UBRememberLocalMapping(new_primary_id, new_primary_addr, total_size);
    UBShmControlBlock *new_ctrl = (UBShmControlBlock *)new_primary_addr;

    InitUBSMemControlBlock(new_ctrl, sz.total_size, sz.clog_size, sz.csnlog_size,
                          sz.xmin_size, sz.snapshot_size);
    InitUBSMemBuffer(new_ctrl, new_primary_addr);

    uint64 clog_offset = sizeof(UBShmControlBlock);
    uint64 csnlog_offset = sizeof(UBShmControlBlock) + sz.clog_size;
    char *old_clog_ptr = old_primary_addr + clog_offset;
    char *new_clog_ptr = new_primary_addr + clog_offset;
    char *old_csnlog_ptr = old_primary_addr + csnlog_offset;
    char *new_csnlog_ptr = new_primary_addr + csnlog_offset;

    size_t copied_bytes = 0;
    if (UBSMemVerification(old_primary_addr)) {
        bool memcpy_ok = true;

        if (!UBSBufferSafeMemcpy(new_clog_ptr, old_clog_ptr, sz.clog_size)) {
            ereport(WARNING, (errmsg("failed to copy CLOG data when syncing UB shared memory from old primary")));
            memcpy_ok = false;
        } else {
            copied_bytes += sz.clog_size;
        }

        if (!UBSBufferSafeMemcpy(new_csnlog_ptr, old_csnlog_ptr, sz.csnlog_size)) {
            ereport(WARNING, (errmsg("failed to copy CSNLOG data when syncing UB shared memory from old primary")));
            memcpy_ok = false;
        } else {
            copied_bytes += sz.csnlog_size;
        }

        if (!memcpy_ok) {
            ereport(WARNING, (errmsg("UB shared memory sync from old primary partially failed, "
                                     "using initialized new-primary buffers")));
        }
    }

    g_instance.shmem_cxt.UBTxnCachePtr = new_primary_addr;
    UBRefreshTxnCachePointers();
    if (old_primary_addr != new_primary_addr) {
        (void)UBUnmapLocalMapping(old_primary_addr, total_size, "old primary");
    }
    /* New primary UB memory is fully initialized, re-enable access */
    g_instance.shmem_cxt.UBMemAccessEnabled.store(true, std::memory_order_release);
    ereport(LOG, (errmsg("UB shared memory sync from old primary finished: old=%d, new=%d, copied=%lu bytes",
                         old_primary_id, new_primary_id, (unsigned long)copied_bytes)));
    
    return true;
}

/* USE_UB_TXN_CACHE - END */
