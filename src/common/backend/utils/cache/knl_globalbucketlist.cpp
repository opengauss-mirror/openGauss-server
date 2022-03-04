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
#include "postgres.h"
#include "utils/atomic.h"
#include "utils/knl_globalbucketlist.h"
#include "utils/knl_globaldbstatmanager.h"

void GlobalBucketList::Init(int nbuckets)
{
    m_nbuckets = nbuckets;
    Assert(m_bucket_entry == NULL);
    m_bucket_entry = (BucketEntry *)palloc0(sizeof(BucketEntry) * (uint)m_nbuckets);
    DLInitList(&m_active_bucket_list);
    SpinLockInit(&m_active_bucket_list_lock);
    for (int i = 0; i < nbuckets; i++) {
        m_bucket_entry[i].elem.dle_val = &m_bucket_entry[i];
    }
    m_elem_count = 0;
}
void GlobalBucketList::AddHeadToBucket(Index hash_index, Dlelem *elem)
{
    Dllist *bucket = &(m_bucket_entry[hash_index].cc_bucket);
    /* active bucket */
    if (DLIsNIL(bucket)) {
        SpinLockAcquire(&(m_active_bucket_list_lock));
        DLAddHead(&m_active_bucket_list, &m_bucket_entry[hash_index].elem);
        SpinLockRelease(&(m_active_bucket_list_lock));
    }
    DLAddHead(bucket, elem);
    pg_atomic_fetch_add_u64(&m_elem_count, 1);
}

void GlobalBucketList::RemoveElemFromBucket(Dlelem *elem)
{
    Dllist *bucket = elem->dle_list;
    DLRemove(elem);
    /* inactive bucket */
    if (DLIsNIL(bucket)) {
        BucketEntry *bucket_obj = (BucketEntry *)(bucket);
        Assert(&bucket_obj->cc_bucket == bucket);
        Assert(DLGetListHdr(&bucket_obj->elem) == &m_active_bucket_list);
        SpinLockAcquire(&(m_active_bucket_list_lock));
        DLRemove(&bucket_obj->elem);
        SpinLockRelease(&(m_active_bucket_list_lock));
    }
    pg_atomic_fetch_sub_u64(&m_elem_count, 1);
}

/*
 * @return INVALID_INDEX means there are no active bucket, no need weedout, otherwise return natural number
 */
Index GlobalBucketList::GetTailBucketIndex()
{
    Index tail_index = INVALID_INDEX;
    SpinLockAcquire(&(m_active_bucket_list_lock));
    if (!DLIsNIL(&m_active_bucket_list)) {
        Dlelem *elt = DLGetTail(&m_active_bucket_list);
        BucketEntry *bucket_entry = (BucketEntry *)DLE_VAL(elt);
        tail_index = bucket_entry - m_bucket_entry;
    }
    SpinLockRelease(&(m_active_bucket_list_lock));
    return tail_index;
}