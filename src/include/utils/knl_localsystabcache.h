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
 * knl_localsystabcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localsystabcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALSYSTABCACHE_H
#define KNL_LOCALSYSTABCACHE_H
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/knl_localsystupcache.h"

using FetTupleFrom = HeapTuple (*)(CatCList *, int);

class LocalSysTabCache : public BaseObject {
public:
    /* common interface */
    LocalSysTabCache()
    {
        m_is_inited = false;
        local_systupcaches = NULL;
        CatCacheNeedEOXActWork = false;
    }

    void ResetInitFlag(bool include_shared)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            if (local_systupcaches[cache_id] == NULL) {
                continue;
            }
            if (!include_shared && local_systupcaches[cache_id]->GetCCRelIsShared()) {
                continue;
            }
            local_systupcaches[cache_id]->ResetInitFlag();
        }
        m_is_inited = false;
    }

    void AtEOXact_CatCache(bool isCommit)
    {
        if (!CatCacheNeedEOXActWork) {
            return;
        }
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            local_systupcaches[cache_id]->AtEOXact_CatCache(isCommit);
        }
        CatCacheNeedEOXActWork = false;
    }

    /* systup may be shared table cache. not cleaned when cleardb */
    MemoryContext GetLocalTupCacheMemoryCxt(Oid cache_id)
    {
        return local_systupcaches[cache_id]->GetMemoryCxt();
    }

    // call when rebuild db
    void ReleaseGlobalRefcount(bool include_shared)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            if (local_systupcaches[cache_id] == NULL) {
                continue;
            }
            if (!include_shared && local_systupcaches[cache_id]->GetCCRelIsShared()) {
                continue;
            }
            local_systupcaches[cache_id]->ReleaseGlobalRefcount();
        }
    }

    void ResetCatalogCaches()
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            local_systupcaches[cache_id]->ResetCatalogCache();
        }
    }

    void CatalogCacheFlushCatalogLocal(Oid rel_oid)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            Assert(cacheinfo[cache_id].reloid == local_systupcaches[cache_id]->GetCCRelOid() && m_is_inited);
            if (cacheinfo[cache_id].reloid == rel_oid) {
                /* Yes, so flush all its contents */
                local_systupcaches[cache_id]->ResetCatalogCache();
                /* Tell inval.c to call syscache callbacks for this cache */
                /* sessionsyscachecallback called by SessionCatCacheCallBack */
                CallThreadSyscacheCallbacks(cache_id, 0);
            }
        }
    }

    void CatalogCacheFlushCatalogGlobal(Oid db_id, Oid rel_oid, bool is_commit)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            Assert(cacheinfo[cache_id].reloid == local_systupcaches[cache_id]->GetCCRelOid());
            if (cacheinfo[cache_id].reloid == rel_oid) {
                local_systupcaches[cache_id]->ResetGlobal(db_id, is_commit);
            }
        }
    }

    void CatCacheCallBack(Oid rel_oid)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            Assert(cacheinfo[cache_id].reloid == local_systupcaches[cache_id]->GetCCRelOid() && m_is_inited);
            if (cacheinfo[cache_id].reloid == rel_oid) {
                CallThreadSyscacheCallbacks(cache_id, 0);
            }
        }
    }

    void SessionCatCacheCallBack(Oid rel_oid)
    {
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            Assert(cacheinfo[cache_id].reloid == local_systupcaches[cache_id]->GetCCRelOid() && m_is_inited);
            if (cacheinfo[cache_id].reloid == rel_oid) {
                CallSessionSyscacheCallbacks(cache_id, 0);
            }
        }
    }

    void CacheIdHashValueInvalidateLocal(int cache_id, uint32 hash_value)
    {
        local_systupcaches[cache_id]->HashValueInvalidateLocal(hash_value);
    }

    void CacheIdHashValueInvalidateGlobal(Oid db_id, int cache_id, uint32 hash_value, bool is_commit)
    {
        local_systupcaches[cache_id]->HashValueInvalidateGlobal(db_id, hash_value, is_commit);
    }

    void PrepareToInvalidateCacheTuple(Relation relation, HeapTuple tuple, HeapTuple newtuple,
                                       void (*function)(int, uint32, Oid))
    {
        Oid reloid;
        CACHE1_elog(DEBUG2, "RelationHeapTupleInvalidate: called");
        /*
         * sanity checks
         */
        Assert(RelationIsValid(relation));
        Assert(HeapTupleIsValid(tuple));
        reloid = RelationGetRelid(relation);
        /* ----------------
         *	for each cache
         *	   if the cache contains tuples from the specified relation
         *		   compute the tuple's hash value(s) in this cache,
         *		   and call the GlobalCatalogCacheIdInvalidate.
         * ----------------
         */
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            Assert(cacheinfo[cache_id].reloid == local_systupcaches[cache_id]->GetCCRelOid() && m_is_inited);
            if (cacheinfo[cache_id].reloid != reloid) {
                continue;
            }
            local_systupcaches[cache_id]->PrepareToInvalidateCacheTuple(tuple, newtuple, function);
        }
        CatCacheNeedEOXActWork = true;
    }

    void CreateObject();
    void CreateCatBuckets();
    void Init()
    {
        if (m_is_inited) {
            return;
        }
        for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
            local_systupcaches[cache_id]->Init();
        }
        m_is_inited = true;
    }

    /*
     *	GetCatCacheHashValue
     *
     *		Compute the hash value for a given set of search keys.
     *
     * The reason for exposing this as part of the API is that the hash value is
     * exposed in cache invalidation operations, so there are places outside the
     * LocalCatCache code that need to be able to compute the hash values.
     */
    uint32 GetCatCacheHashValue(int cache_id, Datum v1, Datum v2, Datum v3, Datum v4)
    {
        return local_systupcaches[cache_id]->GetCatCacheHashValue(v1, v2, v3, v4);
    }
    const TupleDesc GetCCTupleDesc(int cache_id)
    {
        return local_systupcaches[cache_id]->GetCCTupleDesc();
    }

    const LocalSysTupCache *GetLocalSysTupCache(int cache_id)
    {
        return local_systupcaches[cache_id];
    }
    /* search interface */
    HeapTuple SearchTuple(int cache_id, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2)
    {
        return SearchTupleN(cache_id, local_systupcaches[cache_id]->GetCCNKeys(), v1, v2, v3, v4, level);
    }

    /*
     * SearchTupleN() are SearchTuple() versions for a specific number of
     * arguments. The compiler can inline the body and unroll loops, making them a
     * bit faster than SearchTuple().
     */
    HeapTuple SearchTuple1(int cache_id, Datum v1)
    {
        return SearchTupleN(cache_id, 1, v1, 0, 0, 0);
    }

    HeapTuple SearchTuple2(int cache_id, Datum v1, Datum v2)
    {
        return SearchTupleN(cache_id, 2, v1, v2, 0, 0);
    }
    HeapTuple SearchTuple3(int cache_id, Datum v1, Datum v2, Datum v3)
    {
        return SearchTupleN(cache_id, 3, v1, v2, v3, 0);
    }
    HeapTuple SearchTuple4(int cache_id, Datum v1, Datum v2, Datum v3, Datum v4)
    {
        return SearchTupleN(cache_id, 4, v1, v2, v3, v4);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * Specific SearchTuple Function to support ProcedureCreate!
     */
    HeapTuple SearchTupleForProcAllArgs(Datum v1, Datum v2, Datum v3, Datum v4, Datum proArgModes)
    {
        LocalCatCTup *ct = local_systupcaches[PROCALLARGS]->SearchLocalCatCTupleForProcAllArgs(
            v1, v2, v3, v4, proArgModes);
        if (ct == NULL) {
            return NULL;
        }
        return &ct->global_ct->tuple;
    }
#endif

    CatCList *SearchCatCList(int cache_id, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2)
    {
        Assert(m_is_inited);
        LocalCatCList *tuples = local_systupcaches[cache_id]->SearchLocalCatCList(nkeys, v1, v2, v3, v4, level);
        CatCList *cl = (CatCList *)tuples;
        return cl;
    }

    /* catcahe manage struct */
    LocalSysTupCache **local_systupcaches;
private:
    HeapTuple SearchTupleN(int cache_id, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2)
    {
        Assert(m_is_inited);
        LocalCatCTup *ct = local_systupcaches[cache_id]->SearchLocalCatCTuple(nkeys, v1, v2, v3, v4, level);
        if (ct == NULL) {
            return NULL;
        }
        return &ct->global_ct->tuple;
    }

    bool m_is_inited;
    bool CatCacheNeedEOXActWork;
};
#endif