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
 * knl_globalsystupcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalsystupcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

/*
 * Given a hash value and the size of the hash table, find the bucket
 * in which the hash value belongs. Since the hash table must contain
 * a power-of-2 number of elements, this is a simple bitmask.
 */
#ifndef KNL_GLOBALSYSTUPCACHE_H
#define KNL_GLOBALSYSTUPCACHE_H
#include "utils/atomic.h"
#include "access/htup.h"
#include "utils/knl_globalsyscache_common.h"
#include "utils/relcache.h"
#include "access/skey.h"
#include "utils/syscache.h"
#define CATCACHE_MAXKEYS 4

#ifdef CACHEDEBUG
#define CACHE1_elog(a, b) ereport(a, (errmsg(b)))
#define CACHE2_elog(a, b, c) ereport(a, (errmsg(b, c)))
#define CACHE3_elog(a, b, c, d) ereport(a, (errmsg(b, c, d)))
#define CACHE4_elog(a, b, c, d, e) ereport(a, (errmsg(b, c, d, e)))
#define CACHE5_elog(a, b, c, d, e, f) ereport(a, (errmsg(b, c, d, e, f)))
#define CACHE6_elog(a, b, c, d, e, f, g) ereport(a, (errmsg(b, c, d, e, f, g)))
#else
#define CACHE1_elog(a, b)
#define CACHE2_elog(a, b, c)
#define CACHE3_elog(a, b, c, d)
#define CACHE4_elog(a, b, c, d, e)
#define CACHE5_elog(a, b, c, d, e, f)
#define CACHE6_elog(a, b, c, d, e, f, g)
#endif
/* function computing a datum's hash */
typedef uint32 (*CCHashFN)(Datum datum);

/* function computing equality of two datums */
typedef bool (*CCFastEqualFN)(Datum a, Datum b);

/*
 * Global SysCache object definitions, plase understand them in design of CatCache
 *  - GlobalSysTupCache -> CatCache
 *      cc_bucket: an array of bucket with DList as its array element
 *      cc_list: a due-list with GlobalCatCList as its list element
 *  - GlobalCatCTup - CatCTuple
 *  - GLobalCatCList - > CatCList
 */
class GlobalSysTupCache;

/*
 * Refers to "CatCTup" in none-GSC mode, normally consider it as wrapper data structure
 * of HeapTuple of catlaog table that catched in GSC, most of fields are similar with those
 * declared in CatCTup
 *
 * Briefly, GlobalCatCTup describes as a cached catlog tuple for GSC
 */
struct GlobalCatCTup {
    int ct_magic; /* for identifying CatCTup entries */
#define CT_MAGIC 0x57261502
    bool canInsertGSC;         /* ddl tuple? */
    bool dead;                       /* dead flag, swapout or invalid */
    uint32 hash_value; /* hash value for this tuple's keys */

    /* Pointer to its belonging GlobalSysTupCache object */
    GlobalSysTupCache *my_cache;

    /* number of active references */
    pg_atomic_uint64 refcount;

    /*
     * Lookup keys for the entry. By-reference datums point into the tuple for
     * positive cache entries
     */
    Datum keys[CATCACHE_MAXKEYS];

    /*
     * Each tuple in a cache is a member of a Dllist that stores the elements
     * of its hash bucket.
     */
    Dlelem cache_elem; /* list member of per-bucket list */
    HeapTupleData tuple;      /* tuple management header */

    void Release();
};

/*
 * Refers to "CatCList" in none-GSC mode, normally consider it as wrapper data structure
 * of HeapTuple of catlaog table that catched in GSC, most of fields are similar with those
 * declared in CatCTup
 *
 * Briefly, GlobalCatCList works as a partial search result list (int GlobalCatCTup) which
 * satisify first N key of catalog index
 */
struct GlobalCatCList {
    int cl_magic; /* for identifying CatCList entries */
#define CL_MAGIC 0x52765103
    uint32 hash_value; /* hash value for lookup keys */
    bool ordered;               /* members listed in index order? */
    bool canInsertGSC;         /* contain ddl tuple? */
    short nkeys;                /* number of lookup keys specified */
    int n_members;              /* number of member tuples */
    GlobalSysTupCache *my_cache; /* used to free when palloc fail */
    pg_atomic_uint64 refcount;   /* number of active references */

    /*
     * Lookup keys for the entry, with the first nkeys elements being valid.
     * All by-reference are separately allocated.
     */
    Datum keys[CATCACHE_MAXKEYS];
    Dlelem cache_elem; /* list member of per-catcache list */
    GlobalCatCTup *members[FLEXIBLE_ARRAY_MEMBER];

    void Release();
};

/*
 * A wrapper data structure to keep relation related information
 *
 * Reminding! we may remove it later as does not refers a concrete module, just for function
 * invoking pararm convience
 */
typedef struct CatTupRelInfoMsg {
    const char *cc_relname;                       /* name of relation the tuples come from */
    TupleDesc cc_tupdesc;                         /* tuple descriptor (copied from reldesc) */
    CCHashFN cc_hashfunc[CATCACHE_MAXKEYS];       /* hash function for each key */
    CCFastEqualFN cc_fastequal[CATCACHE_MAXKEYS]; /* fast equal function for each key */
    Oid cc_reloid;                                /* OID of relation the tuples come from */
    Oid cc_indexoid;                              /* OID of index matching cache keys */
    // int cc_ntup;                                  /* # of tuples currently in this cache */
    int cc_nkeys;                          /* # of keys (1..CATCACHE_MAXKEYS) */
    int cc_keyno[CATCACHE_MAXKEYS];        /* AttrNumber of each key */
    bool cc_relisshared;                   /* is relation shared across databases? */
} CatTupRelInfoMsg;

/*
 * GSC CatCache search type
 */
enum FIND_TYPE {
    SEARCH_TUPLE_SKIP,
    SCAN_TUPLE_SKIP,
    SCAN_LIST_SKIP,
    PGATTR_LIST_SKIP,
    PROC_LIST_SKIP
};

/*
 * Play a wrapper data structure for function parameters for GlobalCatCtup search
 * routines, most used for internal
 */
typedef struct InsertCatTupInfo {
    FIND_TYPE find_type;
    Datum *arguments;
    HeapTuple ntp;
    uint32 hash_value;
    Index hash_index;
    int16 attnum;
    CatalogRelationBuildParam *catalogDesc;

    /*
     * Flag to indicate current thread holds the lock of GlobalSysTupCache::m_concurrent_lock
     */
    bool has_concurrent_lock;

    /*
     * Flag to indicate if current return GlobalCatCTuple can be insert into GSC, normally
     * it is affected by has_concurrent_lock, is_exclusive
     */
    bool canInsertGSC; /* output param */

    /*
     * Flag to indicate store the tuple global or local in GSC
     */
    bool is_exclusive;
} InsertCatTupInfo;

/*
 * Play a wrapper data structure for function parameters for GlobalCatClist search routines,
 * most used for internal
 */
typedef struct InsertCatListInfo {
    List *ctlist;       /* for output fields */
    uint32 hash_value;

    /* partial search's length of arguments array */
    Datum *arguments;   /* array */
    int nkeys;          /* length */

    /*
     * Flag to indicate current thread holds the lock of GlobalSysTupCache::m_concurrent_lock
     */
    bool has_concurrent_lock;

    /*
     * Flag to indicate whether the search result is ordered
     */
    bool ordered;  /* for output fields */

    /*
     * Flag to indicate if current return GlobalCatCList can be insert into GSC, normally
     * it is affected by has_concurrent_lock, is_exclusive, GlobalCatCTup's canInsertGSC
     */
    bool canInsertGSC;  /* output param */

    /*
     * Flag to indicate store the tuple global or local in GSC
     */
    bool is_exclusive;
} InsertCatListInfo;

/*
 * Refers to "CatCache" in none-GSC mode, normally consider it as cache manager for catlog,
 * a GlobalCatCache object refers one catalog table
 */
class GlobalSysTupCache : public BaseObject {
public:
    GlobalSysTupCache(Oid dbOid, int cache_id, bool is_shared,
        struct GlobalSysDBCacheEntry *entry);
    void SetStatInfoPtr(volatile uint64* tup_count, volatile uint64 *tup_space);

    /*
     * Function to return statinfo for CatCTups stored in cc_bucket
     */
    List *GetGlobalCatCTupStat();
    void ReleaseGlobalCatCTup(GlobalCatCTup *ct);
    void ReleaseGlobalCatCList(GlobalCatCList *cl);

    /* common inferface */
    void HashValueInvalidate(uint32 hash_value);
    template <bool force>
    void ResetCatalogCache();

    void RemoveAllTailElements();

    /*
     * GlobalSysCache major search interfaces from user side:
     *   1. SearchTuple(hsah_value, args)
     *   2. SearchTupleFromFile(hsah_value, args, is_exclusive)
     *   3. SearchList(hash_value, nkeys, agrs)
     *   4. SearchListFromFile(hash_value, nkeys, agrs, is_exclusive)
     */
    GlobalCatCTup *SearchTuple(uint32 hash_value, Datum *arguments)
    {
        return SearchTupleInternal(hash_value, arguments);
    }

    GlobalCatCTup *SearchTupleFromFile(uint32 hash_value, Datum *arguments, bool is_exclusive);

    GlobalCatCList *SearchList(uint32 hash_value, int nkeys, Datum *arguments)
    {
        return SearchListInternal(hash_value, nkeys, arguments);
    }

    GlobalCatCList *SearchListFromFile(uint32 hash_value, int nkeys, Datum *arguments, bool is_exclusive);


    /* simple inline functions */
    inline const char *GetCCRelName()
    {
        return m_relinfo.cc_relname;
    }

    inline const bool CCRelIsShared()
    {
        return m_relinfo.cc_relisshared;
    }

    inline TupleDesc GetCCTupleDesc()
    {
        return m_relinfo.cc_tupdesc;
    }

    inline const CCFastEqualFN *GetCCFastEqual()
    {
        return m_relinfo.cc_fastequal;
    }

    inline const CCHashFN *GetCCHashFunc()
    {
        return m_relinfo.cc_hashfunc;
    }

    inline Oid GetCCRelOid()
    {
        return m_relinfo.cc_reloid;
    }

    void Init();
    inline bool Inited()
    {
        return m_isInited;
    }

    inline uint64 GetDeadNum()
    {
        return m_dead_cts.GetLength();
    }

#ifndef ENABLE_MULTIPLE_NODES
    GlobalCatCTup *SearchTupleWithArgModes(uint32 hash_value, Datum *arguments, oidvector* argModes);
    GlobalCatCTup *SearchTupleFromFileWithArgModes(
        uint32 hash_value, Datum *arguments, oidvector* argModes, bool is_disposable);
    GlobalCatCTup *SearchTupleMissWithArgModes(InsertCatTupInfo *tup_info, oidvector* argModes);
#endif

    bool enable_rls;
private:
    /*
     * Note: functions with "Search" prefix do search + insert if target tuple is not found,
     *       functions with "Find" prefix  do search only
     */
    GlobalCatCTup *SearchTupleInternal(uint32 hash_value, Datum *arguments);
    GlobalCatCTup *SearchTupleMiss(InsertCatTupInfo *tup_info);
    GlobalCatCTup *SearchMissFromProcAndAttribute(InsertCatTupInfo *tup_info);
    /* used by searchtupleinternal */
    GlobalCatCTup *FindSearchKeyTupleFromCache(InsertCatTupInfo *tup_info, int *location);
    /* used by searchtuplemiss */
    GlobalCatCTup *FindScanKeyTupleFromCache(InsertCatTupInfo *tup_info);
    /* used by SearchBuiltinProcCacheList */
    GlobalCatCTup *FindHashTupleFromCache(InsertCatTupInfo *tup_info);
    /* used by searchlistinternal when scan */
    GlobalCatCTup *FindSameTupleFromCache(InsertCatTupInfo *tup_info);
    /* used by SearchPgAttributeCacheList */
    GlobalCatCTup *FindPgAttrTupleFromCache(InsertCatTupInfo *tup_info);
    inline GlobalCatCTup *FindTupleFromCache(InsertCatTupInfo *tup_info)
    {
        Assert(tup_info->find_type != SEARCH_TUPLE_SKIP);
        switch (tup_info->find_type) {
            case SCAN_LIST_SKIP:
                return FindSameTupleFromCache(tup_info);
            case PGATTR_LIST_SKIP:
                return FindPgAttrTupleFromCache(tup_info);
            case PROC_LIST_SKIP:
                return FindHashTupleFromCache(tup_info);
            case SCAN_TUPLE_SKIP:
                return FindScanKeyTupleFromCache(tup_info);
            default:
                Assert(false);
                return NULL;
        }
        return NULL;
    }

    GlobalCatCList *SearchListInternal(uint32 hash_value, int nkeys, Datum *arguments);
    GlobalCatCList *SearchListMiss(InsertCatListInfo *list_info);
    GlobalCatCList *CreateCatCacheList(InsertCatListInfo *list_info);
    GlobalCatCList *InsertListIntoCatCacheList(InsertCatListInfo *list_info, GlobalCatCList *cl);
    GlobalCatCTup *InsertHeapTupleIntoGlobalCatCache(InsertCatTupInfo *tup_info);
    GlobalCatCTup *InsertHeapTupleIntoCatCacheInSingle(InsertCatTupInfo *tup_info);
    GlobalCatCTup *InsertHeapTupleIntoCatCacheInList(InsertCatTupInfo *tup_info);
    GlobalCatCTup *InsertHeapTupleIntoLocalCatCache(InsertCatTupInfo *tup_info);
    void ReleaseTempList(const List *ctlist);
    void SearchPgAttributeCacheList(InsertCatListInfo *list_info);
    void SearchBuiltinProcCacheList(InsertCatListInfo *list_info);
    GlobalCatCList *FindListInternal(uint32 hash_value, int nkeys, Datum *arguments, int *location);
    
    void FreeDeadCts();
    void HandleDeadGlobalCatCTup(GlobalCatCTup *ct);
    void RemoveTailTupleElements(Index hash_index);

    void FreeDeadCls();
    void HandleDeadGlobalCatCList(GlobalCatCList *cl);
    void RemoveTailListElements();

    void FreeGlobalCatCList(GlobalCatCList *cl);

    /* when initdb, this func call first */
    void InitCacheInfo(Oid reloid, Oid indexoid, int nkeys, const int *key, int nbuckets);
    void InitHashTable();
    /* when initdb, this func call second after init relcache */
    void InitRelationInfo();

    uint32 GetGlobalCatCachehashValue(Datum v1, Datum v2, Datum v3, Datum v4);
    void InsertBuiltinFuncInBootstrap();

    void InitInsertCatTupInfo(InsertCatTupInfo *tup_info, HeapTuple ntp, Datum *arguments);
    void CopyTupleIntoGlobalCatCTup(GlobalCatCTup *ct, HeapTuple ntp);
    
    Dllist *GetBucket(Index hash_index)
    {
        return &(cc_buckets[hash_index]);
    }

    void AddHeadToCCList(GlobalCatCList *cl);
    void RemoveElemFromCCList(GlobalCatCList *cl);

    void AddHeadToBucket(Index hash_index, GlobalCatCTup *ct);
    void RemoveElemFromBucket(GlobalCatCTup *ct);

    /* Global cache identifier */
    Oid m_dbOid;
    volatile bool m_isInited;
    struct GlobalSysDBCacheEntry *m_dbEntry;

    int m_cache_id;
    int cc_id;                                    /* cache identifier --- see syscache.h */
    DllistWithLock m_dead_cts;
    DllistWithLock m_dead_cls;

    CatTupRelInfoMsg m_relinfo;
    ScanKeyData cc_skey[CATCACHE_MAXKEYS]; /* precomputed key info for
                                            * heap scans */

    /*
     * # of hash buckets in this cache
     *
     * Reminding, we may use GlobalBucketList to unifom the "bucket implementation"
     * GlobalBucketList
     *      - m_bucket_entry
     *      - m_nbuckets
     */
    int cc_nbuckets;
    Dllist *cc_buckets;     /* same to CatCache::cc_bucket */
    Dllist cc_lists;        /* same to CatCache::cc_list */

    pthread_rwlock_t *m_bucket_rw_locks;             // count of lock equal nbucket
    pthread_rwlock_t *m_list_rw_lock;

    /* for pg_proc which has no concurrent lock, or for shared read, we need acquire a rdlock 
     * avoid a ddl when we load a tuple but not insert into gsc. 
     * this means when ddl, should acquire a wrlock before modify gsc.
     * for ddl, first write tuple infomask, then acquire a wrlock and modify gsc */
    pthread_rwlock_t *m_concurrent_lock;

    volatile uint32 *m_is_tup_swappingouts;
    volatile uint32 m_is_list_swappingout;

    volatile uint64 *m_tup_count;
    volatile uint64 *m_tup_space;
    volatile uint64 *m_searches;
    volatile uint64 *m_hits;
    volatile uint64 *m_newloads;
};

#endif