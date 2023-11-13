/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_localsyscache_common.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localsyscache_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALSYSCACHE_COMMON_H
#define KNL_LOCALSYSCACHE_COMMON_H
#include "utils/knl_globalsyscache_common.h"

/*
 * element entry for LSC's RelCache/PartCache
 */
typedef struct LocalBaseEntry {
    Oid oid;
    Dlelem cache_elem;
    bool obj_is_nailed;
} LocalBaseEntry;

typedef struct LocalPartitionEntry : LocalBaseEntry {
    Partition part;
} LocalPartitionEntry;

typedef struct LocalRelationEntry : LocalBaseEntry {
    Relation rel;
} LocalRelationEntry;

/* Saving with an array, slow query efficiency, adding a layer of hash */
#define IBE_HASH_SIZE       0x80 /* 128 */
#define IBE_HASH_MASK       0x7F
static inline uint32 IBE_hash(uint32 value)
{
    Assert((IBE_HASH_SIZE - 1) == IBE_HASH_MASK);
    return value & IBE_HASH_MASK;
}
struct InvalidBaseEntry {
    int count[IBE_HASH_SIZE];
    int size[IBE_HASH_SIZE];
    uint32 *invalid_values[IBE_HASH_SIZE];
    bool is_reset;
    bool has_value;
    InvalidBaseEntry()
    {
        for (int i = 0; i < IBE_HASH_SIZE; i++) {
            count[i] = 0;
            size[i] = 0;
            invalid_values[i] = NULL;
        }
        is_reset = false;
        has_value = false;
    }
    /* base interface */
    void Init()
    {
        /*
         * The memory context LocalSysCacheTopMemoryContext where the object is
         * located is different from the memory context LocalSysCacheShareMemoryContext
         * when init() is called, and LocalSysCacheShareMemoryContext belongs to
         * LocalSysCacheTopMemoryContext.
         * When the thread pool is enabled, when thread binds to a session to exit,
         * CleanUpSession() will be called to reset LocalSysCacheShareMemoryContext.
         *
         * Now, if a session exits in a transaction, CleanUpSession() will be
         * directly called in the current thread to reset LocalSysCacheShareMemoryContext;
         * After unbinding, the thread immediately binds a session to exit.
         * At this point, CleanUpSession() will be called again. Before resetting
         * LocalSysCacheShareMemoryContext in LocalSysDBCacheReSet(),
         * InvalidBaseEntry::ResetInitFlag() will be called. If the memory allocated
         * in Init() is accessed in ResetInitFlag(), "heap-use-after-free" will appear.
         */
        for (int i = 0; i < IBE_HASH_SIZE; i++) {
            count[i] = 0;
            size[i] = 32;
            invalid_values[i] = (uint32 *)palloc0(size[i] * sizeof(uint32));
        }
        has_value = false;
    }
    void ResetInitFlag()
    {
        /* we dont clean invalid_values and size variable if rebuild lsc
         * for catcache, they will be reinited when rebuild catbucket
         * for rel/part, they will never be reinited */
        for (int i = 0; i < IBE_HASH_SIZE; i++) {
            count[i] = 0;
        }
        is_reset = false;
        has_value = false;
    }

    void InsertInvalidValue(uint32 value)
    {
        if (ExistDefValue(value)) {
            return;
        }
        uint32 hash = IBE_hash(value);
        if (count[hash] == size[hash]) {
            size[hash] = size[hash] * 2;
            invalid_values[hash] = (uint32 *)repalloc(invalid_values[hash], size[hash] * sizeof(uint32));
        }
        invalid_values[hash][count[hash]] = value;
        count[hash]++;
        has_value = true;
    }

    bool ExistValue(uint32 value)
    {
        uint32 hash = IBE_hash(value);
        Assert(hash >= 0);
        Assert(hash < IBE_HASH_SIZE);
        uint32 *dest = invalid_values[hash];
        for (int i = 0; i < count[hash]; i++) {
            if (dest[i] == value) {
                return true;
            }
        }
        return false;
    }

    /* rel/partcache */

    bool ExistDefValue(uint32 value)
    {
        return ExistValue(value);
    }

    void InsertInvalidDefValue(uint32 value)
    {
        InsertInvalidValue(value);
    }

    /* catcache */
    void InsertInvalidTupleValue(uint32 value)
    {
        if (is_reset) {
            return;
        }
        InsertInvalidValue(value);
    }

    void ResetCatalog()
    {
        is_reset = true;
    }

    bool ExistTuple(uint32 value)
    {
        if (is_reset) {
            return true;
        }
        return ExistDefValue(value);
    }

    bool ExistList()
    {
        if (is_reset || has_value) {
            return true;
        }
        return false;
    }

    void CopyFrom(InvalidBaseEntry* from)
    {
        for (int i = 0; i < IBE_HASH_SIZE; i++) {
            for (int j = 0; j < from->count[i]; j++) {
                InsertInvalidDefValue(from->invalid_values[i][j]);
            }
        }
    }
};

void StreamTxnContextSaveInvalidMsg(void *stc);
void StreamTxnContextRestoreInvalidMsg(void *stc);
#define EnableLocalSysCache() t_thrd.lsc_cxt.enable_lsc

/*
 * if MAX_LSC_SWAPOUT_RATIO is 0.9, MAX_LSC_FREESIZE_RATIO is 0.8
 * used_space <= threshold * 0.9 < threshold ==> max_used_space = threshold * 0.8
 * total_space > threshold >= total_space * 0.9 ==> max_total_space = threshold / 0.8
 * so max_used_space = max_total_space * 0.72
 * this means 72% memory can be used at the worst sence */
const double MAX_LSC_FREESIZE_RATIO = 0.2;
const double MAX_LSC_SWAPOUT_RATIO = 0.9;
const double MIN_LSC_SWAPOUT_RATIO = 0.7;
#endif