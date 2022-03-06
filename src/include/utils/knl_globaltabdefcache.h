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
 * knl_globaltabdefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globaltabdefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALTABDEFCACHE_H
#define KNL_GLOBALTABDEFCACHE_H
#include "utils/relcache.h"
#include "utils/knl_globalbasedefcache.h"
#include "utils/knl_globalsyscache_common.h"
#include "utils/relmapper.h"

extern bool has_locator_info(GlobalBaseEntry *entry);
class GlobalTabDefCache : public GlobalBaseDefCache {
public:
    GlobalTabDefCache(Oid dbOid, bool is_shared, struct GlobalSysDBCacheEntry *entry);
    ~GlobalTabDefCache() {};

    void Init();
    void Insert(Relation rel, uint32 hash_value);

    GlobalRelationEntry *SearchReadOnly(Oid relOid, uint32 hash_value)
    {
        GlobalBaseDefCache::FreeDeadEntrys<true>();
        GlobalRelationEntry *entry =
                (GlobalRelationEntry *)GlobalBaseDefCache::SearchReadOnly(relOid, hash_value);
        return entry;
    }

    template <bool force>
    inline void ResetRelCaches()
    {
        if (!m_is_inited) {
            return;
        }
        GlobalBaseDefCache::ResetCaches<true, force>();
        GlobalBaseDefCache::FreeDeadEntrys<true>();
    }

    inline uint64 GetSysCacheSpaceNum()
    {
        return m_base_space;
    }

    inline void Invalidate(Oid dbOid, Oid relOid)
    {
        GlobalBaseDefCache::Invalidate<true>(dbOid, relOid);
    }

    inline void InvalidateRelationNodeList()
    {
        GlobalBaseDefCache::InvalidateRelationNodeListBy<true>(has_locator_info);
    }

    TupleDesc GetPgClassDescriptor();

    TupleDesc GetPgIndexDescriptor();

    List *GetTableStats(Oid rel_oid);

private:

    GlobalRelationEntry *CreateEntry(Relation rel);

    TupleDesc m_pgclassdesc;
    TupleDesc m_pgindexdesc;
    pthread_mutex_t *m_catalog_lock;
};
#endif