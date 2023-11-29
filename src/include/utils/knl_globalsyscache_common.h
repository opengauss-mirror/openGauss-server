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
 * knl_globalsyscache_common.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalsyscache_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALSYSCACHE_COMMON_H
#define KNL_GLOBALSYSCACHE_COMMON_H

#include "catalog/pg_class.h"
#include "nodes/memnodes.h"
#include "utils/knl_globalbucketlist.h"

/*
 * Given a hash value and the size of the hash table, find the bucket
 * in which the hash value belongs. Since the hash table must contain
 * a power-of-2 number of elements, this is a simple bitmask.
 */
#define HASH_INDEX(h, sz) ((Index)((h) & ((sz)-1)))


/*
 * for tuple uncommitted or being deleted, we don't sure whether it will be committed or aborted,
 * so just store them into local cache, and invalid them by send message
 */
inline bool CanTupleInsertGSC(HeapTuple tuple)
{
    if (tuple->t_tableOid == InvalidOid) {
        // this is a heapformtuple
        Assert(tuple->tupTableType == HEAP_TUPLE);
        Assert(tuple->t_bucketId == InvalidBktId);
        Assert(tuple->t_self.ip_blkid.bi_hi == InvalidBlockNumber >> 16);
        Assert(tuple->t_self.ip_blkid.bi_lo == (InvalidBlockNumber & 0xffff));
        Assert(tuple->t_self.ip_posid == InvalidOffsetNumber);
        return true;
    }
    bool can_insert_into_gsc =
        HeapTupleHeaderXminCommitted(tuple->t_data) && !TransactionIdIsValid(HeapTupleGetRawXmax(tuple));
    return can_insert_into_gsc;
}

extern void TopnLruMoveToFront(Dlelem *e, Dllist *list, pthread_rwlock_t *lock, int location);

extern bytea *CopyOption(bytea *options);
extern TupleDesc CopyTupleDesc(TupleDesc tupdesc);
Relation CopyRelationData(Relation newrel, Relation rel, MemoryContext rules_cxt, MemoryContext rls_cxt,
                          MemoryContext index_cxt);

void AcquireGSCTableReadLock(bool *has_concurrent_lock, pthread_rwlock_t *concurrent_lock);
void ReleaseGSCTableReadLock(bool *has_concurrent_lock, pthread_rwlock_t *concurrent_lock);

enum GlobalObjDefEntry{GLOBAL_RELATION_ENTRY, GLOBAL_PARTITION_ENTRY};

struct GlobalBaseEntry {
    GlobalObjDefEntry type;
    Oid oid;
    volatile uint64 refcount;
    Dlelem cache_elem;
    void Release();
    template <bool is_relation>
    static void Free(GlobalBaseEntry *entry);

    void FreeError()
    {
        if (type == GLOBAL_RELATION_ENTRY) {
            Free<true>(this);
        } else {
            Assert(type == GLOBAL_PARTITION_ENTRY);
            Free<false>(this);
        }
    }
};
struct GlobalRelationEntry : public GlobalBaseEntry {
    Relation rel;
    MemoryContext rel_mem_manager;
};
struct GlobalPartitionEntry : public GlobalBaseEntry {
    Partition part;
};

void CopyPartitionData(Partition dest_partition, Partition src_partition);

#ifdef CACHEDEBUG
    #define GSC_CACHE1_elog(b) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b)))
    #define GSC_CACHE2_elog(b, c) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c)))
    #define GSC_CACHE3_elog(b, c, d) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c, d)))
    #define GSC_CACHE4_elog(b, c, d, e) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c, d, e)))
    #define GSC_CACHE5_elog(b, c, d, e, f) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c, d, e, f)))
    #define GSC_CACHE6_elog(b, c, d, e, f, g) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c, d, e, f, g)))
    #define GSC_CACHE7_elog(b, c, d, e, f, g, h) ereport(DEBUG2, (errmodule(MOD_GSC), errmsg(b, c, d, e, f, g, h)))
#else
    #define GSC_CACHE1_elog(b)
    #define GSC_CACHE2_elog(b, c)
    #define GSC_CACHE3_elog(b, c, d)
    #define GSC_CACHE4_elog(b, c, d, e)
    #define GSC_CACHE5_elog(b, c, d, e, f)
    #define GSC_CACHE6_elog(b, c, d, e, f, g)
    #define GSC_CACHE7_elog(b, c, d, e, f, g, h)
#endif

#ifdef ENABLE_LITE_MODE
        const int LOCAL_INIT_RELCACHE_SIZE = 128;
        const int LOCAL_INIT_PARTCACHE_SIZE = 128;
        const int GLOBAL_INIT_RELCACHE_SIZE = LOCAL_INIT_RELCACHE_SIZE << 1;
        const int GLOBAL_INIT_PARTCACHE_SIZE = LOCAL_INIT_PARTCACHE_SIZE << 1;
        const int INIT_DB_SIZE = 64;
        const int MinHashBucketSize = 4;
        const uint64 GLOBAL_DB_MEMORY_MAX = 4 * 1024 * 1024;
#else
        const int LOCAL_INIT_RELCACHE_SIZE = 512;
        const int LOCAL_INIT_PARTCACHE_SIZE = 512;
        const int GLOBAL_INIT_RELCACHE_SIZE = LOCAL_INIT_RELCACHE_SIZE << 1;
        const int GLOBAL_INIT_PARTCACHE_SIZE = LOCAL_INIT_PARTCACHE_SIZE << 1;
        const int INIT_DB_SIZE = 1024;
        const int MinHashBucketSize = 32;
        const uint64 GLOBAL_DB_MEMORY_MAX = 8 * 1024 * 1024;
#endif

const uint64 GLOBAL_DB_MEMORY_MIN = 2 * 1024 * 1024;
const uint64 MemIncreEveryTrans = 1024 * 1024;
const int GLOBAL_BUCKET_DEFAULT_TOP_N = 3;
const int32 MAX_GSC_LIST_LENGTH = 2048;
const int32 MAX_LSC_LIST_LENGTH = 1024;
const int CHUNK_ALGIN_PAD = ALLOC_CHUNKHDRSZ + 8;

inline uint64 GetSwapOutNum(bool blow_threshold, uint64 length)
{
    if (blow_threshold) {
        return length >> 2;
    } else {
        return length >> 1;
    }
}

#endif