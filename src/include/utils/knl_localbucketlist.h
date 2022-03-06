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
 * IDENTIFICATION
 *        src/include/utils/knl_localbucketlist.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef KNL_LOCALBUCKETLIST_H
#define KNL_LOCALBUCKETLIST_H
#include "lib/dllist.h"
#include "utils/knl_globalbucketlist.h"
class LocalBucketList : public BaseObject {
    int m_nbuckets;
    BucketEntry *m_bucket_entry;
    Dllist m_active_bucket_list;
public:
    LocalBucketList()
    {
        m_nbuckets = 0;
        m_bucket_entry = NULL;
    }
    void Init(int nbuckets)
    {
        DLInitList(&m_active_bucket_list);
        m_bucket_entry = (BucketEntry *)palloc0(sizeof(BucketEntry) * nbuckets);
        m_nbuckets = nbuckets;
        for (int i = 0; i < nbuckets; i++) {
            m_bucket_entry[i].elem.dle_val = &m_bucket_entry[i];
        }
    }

    void ResetContent()
    {
        DLInitList(&m_active_bucket_list);
        if (m_bucket_entry == NULL) {
            return;
        }
        errno_t rc = memset_s(m_bucket_entry, sizeof(BucketEntry) * m_nbuckets, 0, sizeof(BucketEntry) * m_nbuckets);
        securec_check(rc, "\0", "\0");
        for (int i = 0; i < m_nbuckets; i++) {
            m_bucket_entry[i].elem.dle_val = &m_bucket_entry[i];
        }
    }

    Dllist *GetActiveBucketList()
    {
        return &m_active_bucket_list;
    }

    Dllist *GetBucket(Index hash_index)
    {
        return &(m_bucket_entry[hash_index].cc_bucket);
    }

    /*
     * @return INVALID_INDEX means there are no active bucket, no need weedout, otherwise return natural number
     */
    Index GetTailBucketIndex()
    {
        Index tail_index = INVALID_INDEX;
        if (!DLIsNIL(&m_active_bucket_list)) {
            Dlelem *elt = DLGetTail(&m_active_bucket_list);
            BucketEntry *bucket_entry = (BucketEntry *)DLE_VAL(elt);
            tail_index = bucket_entry - m_bucket_entry;
        }
        return tail_index;
    }

    void AddHeadToBucket(Index hash_index, Dlelem *elem)
    {
        Dllist *bucket = &(m_bucket_entry[hash_index].cc_bucket);
        // inactive bucket
        if (DLIsNIL(bucket)) {
            DLAddHead(&m_active_bucket_list, &m_bucket_entry[hash_index].elem);
        }
        DLAddHead(bucket, elem);
    }

    void RemoveElemFromBucket(Dlelem *elem)
    {
        Dllist *bucket = elem->dle_list;
        DLRemove(elem);
        // inactive bucket
        if (DLIsNIL(bucket)) {
            BucketEntry *bucket_obj = (BucketEntry *)(bucket);
            Assert(&bucket_obj->cc_bucket == bucket);
            Assert(DLGetListHdr(&bucket_obj->elem) == &m_active_bucket_list);
            DLRemove(&bucket_obj->elem);
        }
    }

    void MoveBucketToHead(Index hash_index)
    {
        DLMoveToFront(&m_bucket_entry[hash_index].elem);
    }

};
#endif
