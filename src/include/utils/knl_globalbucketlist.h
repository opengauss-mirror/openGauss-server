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
 * knl_globalbucketlist.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalbucketlist.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALBUCKETLIST_H
#define KNL_GLOBALBUCKETLIST_H
#include "lib/dllist.h"
#include "access/htup.h"

#define forloopactivebucketlist(bucket_elt, m_active_bucket_list) \
    for ((bucket_elt) = DLGetHead((m_active_bucket_list)); (bucket_elt);)


#define forloopbucket(elt, bucket_elt)                                   \
    BucketEntry *bucket_entry = (BucketEntry *)DLE_VAL(bucket_elt); \
    bucket_elt = DLGetSucc(bucket_elt);                             \
    for ((elt) = DLGetHead(&bucket_entry->cc_bucket); (elt);)

/*
 * base class of GSC/LSC's entry
 */
struct BucketEntry {
    Dllist cc_bucket;
    Dlelem elem;
};

/*
 * A hashtable (key->bucket) to works as a container to hold GSC elements in BucketEntry
 * type format
 *
 * Note: global shared, 
 *
 * Reminding! GlobalBucketList should be changed to Gloabl
 */
class GlobalBucketList : public BaseObject {
    /* Dlist hold active buckets and its protecting lock */
    Dllist  m_active_bucket_list;
    slock_t m_active_bucket_list_lock; 

    /*
     * bucket information of current GSC with length of m_nbuckets
     *
     * Normally, each element type could be:
     *    GlobalSysDBCacheEntry: when use as GlobalSysDBCache:m_bucket_list
     *    RelationData: when use as GlobalTabDefCache:m_bucket_list
     *    PartitionData: when used as GlobalPartDefCache:m_bucket_list
     */
    BucketEntry *m_bucket_entry;    /* array of buckets */
    int          m_nbuckets;        /* array length */

    /* record the memory-usage and number of element in current GSC */
    volatile uint64 m_elem_count;

public:
    GlobalBucketList()
    {
        m_nbuckets = 0;
        m_bucket_entry = NULL;
        m_elem_count = 0;
    }

    /* init function */
    void Init(int nbuckets);

    /*
     * Global bucket list operation functions, add/remove etc.
     */

    /* add elment */
    void AddHeadToBucket(Index hash_index, Dlelem *elem);

    /* remove element */
    void RemoveElemFromBucket(Dlelem *elem);

    /* get tail index */
    Index GetTailBucketIndex();

    inline Dllist *GetBucket(Index hash_index)
    {
        return &(m_bucket_entry[hash_index].cc_bucket);
    }

    inline uint64 GetActiveElementCount()
    {
        return m_elem_count;
    }
};

#endif