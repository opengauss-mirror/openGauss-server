/* -------------------------------------------------------------------------
 *
 * catcache.h
 *	  Low-level catalog cache definitions.
 *
 * NOTE: every catalog cache must have a corresponding unique index on
 * the system table that it caches --- ie, the index must match the keys
 * used to do lookups in this cache.  All cache fetches are done with
 * indexscans (under normal conditions).  The index should be unique to
 * guarantee that there can only be one matching row for a key combination.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/catcache.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CATCACHE_H
#define CATCACHE_H

#include "access/htup.h"
#include "access/skey.h"
#include "lib/dllist.h"
#include "utils/relcache.h"

/*
 *		struct catctup:			individual tuple in the cache.
 *		struct catclist:		list of tuples matching a partial key.
 *		struct catcache:		information for managing a cache.
 *		struct catcacheheader:	information for managing all the caches.
 */

#define CATCACHE_MAXKEYS 4

/* function computing a datum's hash */
typedef uint32 (*CCHashFN)(Datum datum);

/* function computing equality of two datums */
typedef bool (*CCFastEqualFN)(Datum a, Datum b);

typedef struct CatCache {
    int id;                                       /* cache identifier --- see syscache.h */
    int cc_nbuckets;                              /* # of hash buckets in this cache */
    CatCache* cc_next;                            /* link to next catcache */
    const char* cc_relname;                       /* name of relation the tuples come from */
    TupleDesc cc_tupdesc;                         /* tuple descriptor (copied from reldesc) */
    CCHashFN cc_hashfunc[CATCACHE_MAXKEYS];       /* hash function for each key */
    CCFastEqualFN cc_fastequal[CATCACHE_MAXKEYS]; /* fast equal function for each key */
    Oid cc_reloid;                                /* OID of relation the tuples come from */
    Oid cc_indexoid;                              /* OID of index matching cache keys */
    int cc_ntup;                                  /* # of tuples currently in this cache */
    int cc_nkeys;                                 /* # of keys (1..CATCACHE_MAXKEYS) */
    int cc_keyno[CATCACHE_MAXKEYS];               /* AttrNumber of each key */
    bool cc_relisshared;                          /* is relation shared across databases? */
    Dllist cc_lists;                              /* list of CatCList structs */
    ScanKeyData cc_skey[CATCACHE_MAXKEYS];        /* precomputed key info for
                                                   * heap scans */
#ifdef CATCACHE_STATS
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
#endif
    Dllist cc_bucket[FLEXIBLE_ARRAY_MEMBER]; /* hash buckets --- VARIABLE LENGTH ARRAY */
} CatCache;                                  /* VARIABLE LENGTH STRUCT */

typedef struct catctup {
    int ct_magic; /* for identifying CatCTup entries */
#define CT_MAGIC 0x57261502

    uint32 hash_value; /* hash value for this tuple's keys */

    /*
     * Lookup keys for the entry. By-reference datums point into the tuple for
     * positive cache entries, and are separately allocated for negative ones.
     */
    Datum keys[CATCACHE_MAXKEYS];

    /*
     * Each tuple in a cache is a member of a Dllist that stores the elements
     * of its hash bucket.	We keep each Dllist in LRU order to speed repeated
     * lookups.
     */
    Dlelem cache_elem; /* list member of per-bucket list */

    /*
     * A tuple marked "dead" must not be returned by subsequent searches.
     * However, it won't be physically deleted from the cache until its
     * refcount goes to zero.  (If it's a member of a CatCList, the list's
     * refcount must go to zero, too; also, remember to mark the list dead at
     * the same time the tuple is marked.)
     *
     * A negative cache entry is an assertion that there is no tuple matching
     * a particular key.  This is just as useful as a normal entry so far as
     * avoiding catalog searches is concerned.	Management of positive and
     * negative entries is identical.
     */
    int refcount;        /* number of active references */
    bool dead;           /* dead but not yet removed? */
    bool negative;       /* negative cache entry? */
    bool isnailed;       /* indicate if we can reomve this cattup from syscache or not */
    HeapTupleData tuple; /* tuple management header */

    /*
     * The tuple may also be a member of at most one CatCList.	(If a single
     * catcache is list-searched with varying numbers of keys, we may have to
     * make multiple entries for the same tuple because of this restriction.
     * Currently, that's not expected to be common, so we accept the potential
     * inefficiency.)
     */
    struct catclist* c_list; /* containing CatCList, or NULL if none */
    CatCache* my_cache;      /* link to owning catcache */
} CatCTup;

/*
 * A CatCList describes the result of a partial search, ie, a search using
 * only the first K key columns of an N-key cache.	We form the keys used
 * into a tuple (with other attributes NULL) to represent the stored key
 * set.  The CatCList object contains links to cache entries for all the
 * table rows satisfying the partial key.  (Note: none of these will be
 * negative cache entries.)
 *
 * A CatCList is only a member of a per-cache list; we do not currently
 * divide them into hash buckets.
 *
 * A list marked "dead" must not be returned by subsequent searches.
 * However, it won't be physically deleted from the cache until its
 * refcount goes to zero.  (A list should be marked dead if any of its
 * member entries are dead.)
 *
 * If "ordered" is true then the member tuples appear in the order of the
 * cache's underlying index.  This will be true in normal operation, but
 * might not be true during bootstrap or recovery operations. (namespace.c
 * is able to save some cycles when it is true.)
 */
typedef struct catclist {
    int cl_magic; /* for identifying CatCList entries */
#define CL_MAGIC 0x52765103

    uint32 hash_value; /* hash value for lookup keys */

    Dlelem cache_elem; /* list member of per-catcache list */

    /*
     * Lookup keys for the entry, with the first nkeys elements being valid.
     * All by-reference are separately allocated.
     */
    Datum keys[CATCACHE_MAXKEYS];

    int refcount;                            /* number of active references */
    bool dead;                               /* dead but not yet removed? */
    bool isnailed;                           /* indicate if we can reomve this catlist from syscache or not */
    bool ordered;                            /* members listed in index order? */
    short nkeys;                             /* number of lookup keys specified */
    int n_members;                           /* number of member tuples */
    CatCache* my_cache;                      /* link to owning catcache */
    CatCTup** systups;                       /* systups, link to CatCTup for pg; link to GlobalCatCTup for lsc
        dont access this variable directly, 
        fetch element by call t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i) instead */
} CatCList;                                  /* VARIABLE LENGTH STRUCT */

typedef struct CatCacheHeader {
    CatCache* ch_caches; /* head of list of CatCache structs */
    int ch_ntup;         /* # of tuples in all caches */
} CatCacheHeader;

extern void AtEOXact_CatCache(bool isCommit);

extern CatCache* InitCatCache(int id, Oid reloid, Oid indexoid, int nkeys, const int* key, int nbuckets);
extern void InitCatCachePhase2(CatCache* cache, bool touch_index);

extern HeapTuple SearchCatCache(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4, int level);
extern HeapTuple SearchCatCache1(CatCache* cache, Datum v1);
extern HeapTuple SearchCatCache2(CatCache* cache, Datum v1, Datum v2);
extern HeapTuple SearchCatCache3(CatCache* cache, Datum v1, Datum v2, Datum v3);
extern HeapTuple SearchCatCache4(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4);

extern void ReleaseCatCache(HeapTuple tuple);

extern uint32 GetCatCacheHashValue(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4);

extern CatCList* SearchCatCacheList(CatCache* cache, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4);
extern void ReleaseCatCacheList(CatCList* list);

extern void ReleaseTempCatList(const List* volatile ctlist, CatCache* cache);

extern void ResetCatalogCaches(void);
extern void CatalogCacheFlushCatalog(Oid catId);
extern void CatalogCacheIdInvalidate(int cacheId, uint32 hashValue);
extern void PrepareToInvalidateCacheTuple(
    Relation relation, HeapTuple tuple, HeapTuple newtuple, void (*function)(int, uint32, Oid));

extern void PrintCatCacheLeakWarning(HeapTuple tuple);
extern void PrintCatCacheListLeakWarning(CatCList* list);
extern void InsertBuiltinFuncDescInBootstrap();
extern void InsertBuiltinFuncInBootstrap();

#ifndef ENABLE_MULTIPLE_NODES
extern HeapTuple SearchSysCacheForProcAllArgs(Datum v1, Datum v2, Datum v3, Datum v4, Datum proArgModes);
#endif

#endif /* CATCACHE_H */
