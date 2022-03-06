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
 * knl_localbasedefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localbasedefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALBASEDEFCACHE_H
#define KNL_LOCALBASEDEFCACHE_H
#include "utils/knl_globalsysdbcache.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/knl_localbucketlist.h"
#include "utils/knl_localsyscache_common.h"
#include "utils/rel.h"

class LocalBaseDefCache : public BaseObject {
public:
    LocalBaseDefCache()
    {
        m_nbuckets = 0;
        m_db_id = InvalidOid;
    }

    LocalBaseEntry *SearchEntryFromLocal(Oid oid, Index hash_index);

    void CreateDefBucket(size_t size);

    LocalBaseEntry *CreateEntry(Index hash_index, size_t size);
    InvalidBaseEntry invalid_entries;

    template <bool is_relation> 
    void RemoveTailDefElements();

protected:

    int m_nbuckets; /* # of hash buckets in this cache */
    Oid m_db_id;
    LocalBucketList m_bucket_list;
};
#endif