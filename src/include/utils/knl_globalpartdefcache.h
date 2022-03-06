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
 * knl_globalpartdefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalpartdefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALPARTDEFCACHE_H
#define KNL_GLOBALPARTDEFCACHE_H

#include "utils/partcache.h"
#include "utils/knl_globalbasedefcache.h"
#include "utils/knl_globalsyscache_common.h"
#include "nodes/memnodes.h"

/*
 * GlobalPartDefCache refers to PartCache, it is the cache for pg_partition catalog
 * partOid is the oid of subPartition (key of pg_partition) for a partition table
 */
class GlobalPartDefCache : public GlobalBaseDefCache {
public:
    GlobalPartDefCache(Oid dbOid, bool isShared, struct GlobalSysDBCacheEntry *entry);
    ~GlobalPartDefCache() {}

    void Init();
    void Insert(Partition part, uint32 hash_value);

    /*
     * Note: partRelOid is the key entry pg_partition
     */
    GlobalPartitionEntry *SearchReadOnly(Oid partRelOid, uint32 hash_value)
    {
        GlobalBaseDefCache::FreeDeadEntrys<false>();
        GlobalPartitionEntry *entry =
                (GlobalPartitionEntry *)GlobalBaseDefCache::SearchReadOnly(partRelOid, hash_value);
        return entry;
    }
    template <bool force>
    void ResetPartCaches()
    {
        if (!m_is_inited) {
            return;
        }
        GlobalBaseDefCache::ResetCaches<false, force>();
        GlobalBaseDefCache::FreeDeadEntrys<false>();
    }

    inline void Invalidate(Oid dbOid, Oid partRelOid)
    {
        GlobalBaseDefCache::Invalidate<false>(dbOid, partRelOid);
    }

    inline uint64 GetSysCacheSpaceNum()
    {
        return m_base_space;
    }
protected:
    GlobalPartitionEntry *CreateEntry(Partition part);
};

#endif
