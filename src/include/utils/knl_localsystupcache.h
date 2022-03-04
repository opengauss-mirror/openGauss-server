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
 * knl_localsystupcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localsystupcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALSYSTUPCACHE_H
#define KNL_LOCALSYSTUPCACHE_H

#include "utils/knl_globalsysdbcache.h"
#include "utils/knl_globalsystabcache.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/knl_localsyscache_common.h"
#include "utils/syscache.h"

class LocalSysTupCache;
struct LocalCatCTup {
    int ct_magic; /* for identifying CatCTup entries */
#define CT_MAGIC 0x57261502
    uint32 hash_value; /* hash value for this tuple's keys */
    /*
     * Each tuple in a cache is a member of a Dllist that stores the elements
     * of its hash bucket.  We keep each Dllist in LRU order to speed repeated
     * lookups.
     */
    Dlelem cache_elem; /* list member of per-bucket list */
    Datum keys[CATCACHE_MAXKEYS];
    /*
     * A tuple marked "dead" must not be returned by subsequent searches.
     * However, it won't be physically deleted from the cache until its
     * refcount goes to zero.
     *
     * A negative cache entry is an assertion that there is no tuple matching
     * a particular key.  This is just as useful as a normal entry so far as
     * avoiding catalog searches is concerned.  Management of positive and
     * negative entries is identical.
     */
    int refcount;             /* number of active references */
    GlobalCatCTup *global_ct; /* NULL means negative tuple */

    void Release()
    {
        Assert(ct_magic == CT_MAGIC);
        Assert(refcount > 0);
        Assert(global_ct != NULL);
        refcount--;
    }
};

struct LocalCatCList : CatCList {
    GlobalCatCList *global_cl;
    void Release()
    {
        /* Safety checks to ensure we were handed a cache entry */
        Assert(cl_magic == CL_MAGIC);
        Assert(refcount > 0);
        refcount--;
    }
};

class LocalSysTupCache : public BaseObject {
public:
    /* common interface */
    LocalSysTupCache(int cache_id);
    void ResetInitFlag();
    void AtEOXact_CatCache(bool isCommit)
    {
        invalid_entries.ResetInitFlag();
    }
    int GetCCNKeys() const
    {
        return m_relinfo.cc_nkeys;
    }
    int GetCCKeyNo(int index) const
    {
        return m_relinfo.cc_keyno[index];
    }
    Oid GetCCRelOid() const
    {
        return m_relinfo.cc_reloid;
    }
    Oid GetCCIndexOid() const
    {
        return m_relinfo.cc_indexoid;
    }
    bool GetCCRelIsShared()
    {
        return m_relinfo.cc_relisshared;
    }
    const MemoryContext GetMemoryCxt()
    {
        InitPhase2();
        return m_local_mem_cxt;
    }

    const TupleDesc GetCCTupleDesc()
    {
        InitPhase2();
        return m_relinfo.cc_tupdesc;
    }
    /* init funcs */
    void CreateCatBucket();
    void Init()
    {
        Assert(!m_is_inited || m_relinfo.cc_relisshared);
        m_is_inited = true;
    }

    uint32 GetCatCacheHashValue(Datum v1, Datum v2, Datum v3, Datum v4);
    /* search interface */
    LocalCatCTup *SearchLocalCatCTuple(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2)
    {
        InitPhase2();
        return SearchTupleInternal(nkeys, v1, v2, v3, v4, level);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * Specific SearchLocalCatCTuple Function to support ProcedureCreate!
     */
    LocalCatCTup *SearchTupleFromGlobalForProcAllArgs(
        Datum *arguments, uint32 hash_value, Index hash_index, oidvector* argModes);
    LocalCatCTup *SearchLocalCatCTupleForProcAllArgs(Datum v1, Datum v2, Datum v3, Datum v4, Datum proArgModes);
#endif

    LocalCatCList *SearchLocalCatCList(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2)
    {
        InitPhase2();
        return SearchListInternal(nkeys, v1, v2, v3, v4, level);
    }

    void ReleaseGlobalRefcount();
    template <bool reset>
    void FlushGlobalByInvalidMsg(Oid db_id, uint32 hash_value);
    void ResetCatalogCache();
    void ResetGlobal(Oid db_id, bool is_commit)
    {
        if (!is_commit) {
            invalid_entries.ResetCatalog();
            return;
        }
        FlushGlobalByInvalidMsg<true>(db_id, 0);
    }
    void HashValueInvalidateLocal(uint32 hash_value);
    void HashValueInvalidateGlobal(Oid db_id, uint32 hash_value, bool is_commit)
    {
        if (!is_commit) {
            invalid_entries.InsertInvalidDefValue(hash_value);
            return;
        }
        FlushGlobalByInvalidMsg<false>(db_id, hash_value);
    }
    void PrepareToInvalidateCacheTuple(HeapTuple tuple, HeapTuple newtuple, void (*function)(int, uint32, Oid));
    InvalidBaseEntry invalid_entries;
private:
    /*
     *	SearchTupleInternal
     *
     *		This call searches a system cache for a tuple, opening the relation
     *		if necessary (on the first access to a particular cache).
     *
     *		The result is NULL if not found, or a pointer to a HeapTuple in
     *		the cache.	The caller must not modify the tuple, and must call
     *		ReleaseTuple() when done with it.
     *
     * The search key values should be expressed as Datums of the key columns'
     * datatype(s).  (Pass zeroes for any unused parameters.)  As a special
     * exception, the passed-in key for a NAME column can be just a C string;
     * the caller need not go to the trouble of converting it to a fully
     * null-padded NAME.
     */
    LocalCatCTup *SearchTupleInternal(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level);
    LocalCatCTup *SearchTupleFromGlobal(Datum *arguments, uint32 hash_value, Index hash_index, int level);
    LocalCatCList *SearchListInternal(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level);
    LocalCatCList *SearchListFromGlobal(int nkeys, Datum *arguments, uint32 hash_value, int level);

    void FreeLocalCatCList(LocalCatCList *cl);
    void HandleDeadLocalCatCList(LocalCatCList *cl);
    void FreeDeadCls();

    void FreeLocalCatCTup(LocalCatCTup *ct);
    void HandleDeadLocalCatCTup(LocalCatCTup *ct);
    void FreeDeadCts();

    LocalCatCTup *CreateLocalCatCTup(GlobalCatCTup *global_ct, Datum *arguments, uint32 hash_value, Index hash_index);

    void InitPhase2()
    {
        if (unlikely(!m_is_inited_phase2)) {
            InitPhase2Impl();
        }
        if (unlikely(m_global_systupcache->enable_rls)) {
            FlushRlsUserImpl();
        }
    }

    void FlushRlsUserImpl();
    void InitPhase2Impl();

    void RemoveTailTupleElements(Index hash_index);
    void RemoveTailListElements();

    Dllist *GetBucket(Index hash_index)
    {
        return &(cc_buckets[hash_index]);
    }

    bool m_is_inited;
    bool m_is_inited_phase2;
    Oid m_rls_user;
    Dllist m_dead_cts;
    Dllist m_dead_cls;

    /* standard memory context, manage memory only used by current thread */
    MemoryContext m_local_mem_cxt;

    /* catcache manage struct */
    GlobalSysTupCache *m_global_systupcache;

    Oid m_db_id;
    Oid m_cache_id;  /* equal cc_id */
    int cc_id;       /* cache identifier --- see syscache.h */
    CatTupRelInfoMsg m_relinfo;
    int cc_nbuckets; /* # of hash buckets in this cache */
    Dllist cc_lists;                /* list of CatCList structs */
    Dllist *cc_buckets;
    long cc_searches; /* total # searches against this cache */
    long cc_hits;     /* # of matches against existing entry */
    long cc_neg_hits; /* # of matches against negative entry */
    long cc_newloads; /* # of successful loads of new entry */
    /*
     * cc_searches - (cc_hits + cc_neg_hits + cc_newloads) is number of failed
     * searches, each of which will result in loading a negative entry
     */
    long cc_invals;    /* # of entries invalidated from cache */
    long cc_lsearches; /* total # list-searches */
    long cc_lhits;     /* # of matches against existing lists */
};

extern bool CheckPrivilegeOfTuple(HeapTuple ct);
#endif
