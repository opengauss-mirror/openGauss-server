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
 */

#include "utils/knl_localbasedefcache.h"
#include "knl/knl_instance.h"
#include "utils/knl_relcache.h"
#include "utils/knl_catcache.h"
#include "utils/knl_partcache.h"
#include "utils/relmapper.h"
#include "postmaster/autovacuum.h"
#include "pgxc/bucketmap.h"

LocalBaseEntry *LocalBaseDefCache::SearchEntryFromLocal(Oid oid, Index hash_index)
{
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        LocalBaseEntry *entry = (LocalBaseEntry *)DLE_VAL(elt);
        if (unlikely(entry->oid != oid)) {
            continue;
        }
        DLMoveToFront(&entry->cache_elem);
        return entry;
    }
    return NULL;
}

void LocalBaseDefCache::CreateDefBucket(size_t size)
{
    invalid_entries.Init();
    m_nbuckets =  ResizeHashBucket(size, g_instance.global_sysdbcache.dynamic_hash_bucket_strategy);
    m_bucket_list.Init(m_nbuckets);
}

LocalBaseEntry *LocalBaseDefCache::CreateEntry(Index hash_index, size_t size)
{
    MemoryContext old = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    LocalBaseEntry *entry = (LocalBaseEntry *)palloc(size);
    MemoryContextSwitchTo(old);
    DLInitElem(&entry->cache_elem, (void *)entry);
    m_bucket_list.AddHeadToBucket(hash_index, &entry->cache_elem);
    return entry;
}
template <bool is_relation>
void LocalBaseDefCache::RemoveTailDefElements()
{
    Index tail_index = m_bucket_list.GetTailBucketIndex();
    if (unlikely(tail_index == INVALID_INDEX)) {
        return;
    }

    int swapout_count_once = 0;
    int max_swapout_count_once = m_bucket_list.GetBucket(tail_index)->dll_len >> 1;
    for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(tail_index)); elt != NULL;) {
        Dlelem *tmp = elt;
        elt = DLGetPred(elt);
        if (is_relation) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(tmp);
            /* dont remove rel created by current transaction or temp table */
            if (RelationHasReferenceCountZero(entry->rel) && entry->rel->rd_createSubid == InvalidSubTransactionId &&
                entry->rel->rd_newRelfilenodeSubid == InvalidSubTransactionId) {
                Assert(!entry->rel->rd_isnailed);
                Assert(entry->rel->entry == entry);
                /* clear call remove relation */
                RelationClearRelation(entry->rel, false);
                swapout_count_once++;
            } else {
                DLMoveToFront(&entry->cache_elem);
                /* lsc do lru strategy, so the front of this entry are all refered probably, just break */
                break;
            }
        } else {
            LocalPartitionEntry *entry = (LocalPartitionEntry *)DLE_VAL(tmp);
            /* dont remove part created by current transaction or temp table */
            if (PartitionHasReferenceCountZero(entry->part) &&
                entry->part->pd_newRelfilenodeSubid == InvalidSubTransactionId &&
                entry->part->pd_createSubid == InvalidSubTransactionId) {
                Assert(entry->part->entry == entry);
                /* clear call remove partation */
                PartitionClearPartition(entry->part, false);
                swapout_count_once++;
            } else {
                DLMoveToFront(&entry->cache_elem);
                /* lsc do lru strategy, so the front of this entry are all refered probably, just break */
                break;
            }
        }

        /* keep elements as many as possible */
        if (swapout_count_once == max_swapout_count_once) {
            break;
        }
    }

    if (!DLIsNIL(m_bucket_list.GetBucket(tail_index))) {
        m_bucket_list.MoveBucketToHead(tail_index);
    }
}

template void LocalBaseDefCache::RemoveTailDefElements<false>();
template void LocalBaseDefCache::RemoveTailDefElements<true>();
