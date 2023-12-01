/* -------------------------------------------------------------------------
 *
 * catcache.c
 *	  System catalog cache for tuples matching a key.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/catcache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "catalog/pg_description.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attribute.h"
#include "catalog/heap.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#ifdef CATCACHE_STATS
#include "storage/ipc.h" /* for on_proc_exit */
#endif
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/datum.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/fmgrtab.h"
#include "utils/hashutils.h"
#include "utils/inval.h"
#include "utils/knl_catcache.h"
#include "utils/knl_relcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "libpq/md5.h"

/*
 * Given a hash value and the size of the hash table, find the bucket
 * in which the hash value belongs. Since the hash table must contain
 * a power-of-2 number of elements, this is a simple bitmask.
 */
#define HASH_INDEX(h, sz) ((Index)((h) & ((sz)-1)))

/*
 *		variables, macros and other stuff
 */

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

extern Datum pv_builtin_functions(PG_FUNCTION_ARGS);

static inline HeapTuple SearchCatCacheInternal(
    CatCache* cache, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level = DEBUG2);

static HeapTuple SearchCatCacheMiss(
    CatCache* cache, int nkeys, uint32 hashValue, Index hashIndex, Datum v1, Datum v2, Datum v3, Datum v4, int level);

#ifdef CATCACHE_STATS
static void CatCachePrintStats(int code, Datum arg);
#endif
static CatCList* SearchCatCacheListMiss(CatCache* cache, int nkeys, Datum* arguments, uint32 lHashValue);
static void CatCacheRemoveCTup(CatCache* cache, CatCTup* ct);
static void CatCacheRemoveCList(CatCache* cache, CatCList* cl);
static void CatalogCacheInitializeCache(CatCache* cache);
static CatCTup* CatalogCacheCreateEntry(CatCache* cache, HeapTuple ntp, Datum* arguments, uint32 hashValue,
    Index hashIndex, bool negative, bool isnailed = false);


#ifndef ENABLE_MULTIPLE_NODES
#define PROCALLARGS_KEY_NUM 4
static uint32 CatalogCacheComputeTupleHashValueForProcAllArgs(
    int* cc_keyno, TupleDesc cc_tupdesc, CCHashFN *cc_hashfunc, Oid cc_reloid, int nkeys, HeapTuple tuple);
#endif


/*
 *					internal support functions
 */

/*
 * Hash and equality functions for system types that are used as cache key
 * fields.  In some cases, we just call the regular SQL-callable functions for
 * the appropriate data type, but that tends to be a little slow, and the
 * speed of these functions is performance-critical.  Therefore, for data
 * types that frequently occur as catcache keys, we hard-code the logic here.
 * Avoiding the overhead of DirectFunctionCallN(...) is a substantial win, and
 * in certain cases (like int4) we can adopt a faster hash algorithm as well.
 */

static bool chareqfast(Datum a, Datum b)
{
    return DatumGetChar(a) == DatumGetChar(b);
}

static uint32 charhashfast(Datum datum)
{
    return murmurhash32((int32)DatumGetChar(datum));
}

static bool nameeqfast(Datum a, Datum b)
{
    char* ca = NameStr(*DatumGetName(a));
    char* cb = NameStr(*DatumGetName(b));

    return strncmp(ca, cb, NAMEDATALEN) == 0;
}

static uint32 namehashfast(Datum datum)
{
    char* key = NameStr(*DatumGetName(datum));

    return hash_any((unsigned char*)key, strlen(key));
}

static bool int1eqfast(Datum a, Datum b)
{
    return DatumGetInt8(a) == DatumGetInt8(b);
}

static uint32 int1hashfast(Datum datum)
{
    return murmurhash32((int32)DatumGetInt8(datum));
}

static bool int2eqfast(Datum a, Datum b)
{
    return DatumGetInt16(a) == DatumGetInt16(b);
}

static uint32 int2hashfast(Datum datum)
{
    return murmurhash32((int32)DatumGetInt16(datum));
}

static bool int4eqfast(Datum a, Datum b)
{
    return DatumGetInt32(a) == DatumGetInt32(b);
}

static uint32 int4hashfast(Datum datum)
{
    return murmurhash32((int32)DatumGetInt32(datum));
}

static bool int8eqfast(Datum a, Datum b)
{
    return DatumGetInt64(a) == DatumGetInt64(b);
}

static uint32 int8hashfast(Datum datum)
{
    /*
     * The idea here is to produce a hash value compatible with the values
     * produced by hashint4 and hashint2 for logically equal inputs; this is
     * necessary to support cross-type hash joins across these input types.
     * Since all three types are signed, we can xor the high half of the int8
     * value if the sign is positive, or the complement of the high half when
     * the sign is negative.
     */
    int64 val = DatumGetInt64(datum);
    uint32 lohalf = (uint32)val;
    uint32 hihalf = (uint32)((unsigned long int)val >> 32);

    lohalf ^= (val >= 0) ? hihalf : ~hihalf;
    return murmurhash32(lohalf);
}

static bool texteqfast(Datum a, Datum b)
{
    return DatumGetBool(DirectFunctionCall2(texteq, a, b));
}

static uint32 texthashfast(Datum datum)
{
    return DatumGetInt32(DirectFunctionCall1(hashtext, datum));
}

static bool oidvectoreqfast(Datum a, Datum b)
{
    return DatumGetBool(DirectFunctionCall2(oidvectoreq, a, b));
}

static uint32 oidvectorhashfast(Datum datum)
{
    return DatumGetInt32(DirectFunctionCall1(hashoidvector, datum));
}

static bool int2vectoreqfast(Datum a, Datum b)
{
    return DatumGetBool(DirectFunctionCall2(int2vectoreq, a, b));
}

static uint32 int2vectorhashfast(Datum datum)
{
    return DatumGetInt32(DirectFunctionCall1(hashint2vector, datum));
}

static bool uuideqfast(Datum a, Datum b)
{
    return DatumGetBool(DirectFunctionCall2(uuid_eq, a, b));
}
static uint32 uuidhashfast(Datum datum)
{
    return DatumGetInt32(DirectFunctionCall1(uuid_hash, datum));
}

/* Lookup support functions for a type. */
void GetCCHashEqFuncs(Oid keytype, CCHashFN* hashfunc, RegProcedure* eqfunc, CCFastEqualFN* fasteqfunc)
{
    switch (keytype) {
        case BOOLOID:
            *hashfunc = charhashfast;
            *fasteqfunc = chareqfast;
            *eqfunc = F_BOOLEQ;
            break;
        case CHAROID:
            *hashfunc = charhashfast;
            *fasteqfunc = chareqfast;
            *eqfunc = F_CHAREQ;
            break;
        case NAMEOID:
            *hashfunc = namehashfast;
            *fasteqfunc = nameeqfast;
            *eqfunc = F_NAMEEQ;
            break;
        case INT1OID:
            *hashfunc = int1hashfast;
            *fasteqfunc = int1eqfast;
            *eqfunc = F_INT1EQ;
            break;
        case INT2OID:
            *hashfunc = int2hashfast;
            *fasteqfunc = int2eqfast;
            *eqfunc = F_INT2EQ;
            break;
        case INT2VECTOROID:
            *hashfunc = int2vectorhashfast;
            *fasteqfunc = int2vectoreqfast;
            *eqfunc = F_INT2VECTOREQ;
            break;
        case INT4OID:
            *hashfunc = int4hashfast;
            *fasteqfunc = int4eqfast;
            *eqfunc = F_INT4EQ;
            break;
        case INT8OID:
            *hashfunc = int8hashfast;
            *fasteqfunc = int8eqfast;
            *eqfunc = F_INT8EQ;
            break;
        case TEXTOID:
            *hashfunc = texthashfast;
            *fasteqfunc = texteqfast;
            *eqfunc = F_TEXTEQ;
            break;
        case UUIDOID:
            *hashfunc = uuidhashfast;
            *fasteqfunc = uuideqfast;
            *eqfunc = F_UUID_EQ;
            break;
        case OIDOID:
        case REGPROCOID:
        case REGPROCEDUREOID:
        case REGOPEROID:
        case REGOPERATOROID:
        case REGCLASSOID:
        case REGTYPEOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
            *hashfunc = int4hashfast;
            *fasteqfunc = int4eqfast;
            *eqfunc = F_OIDEQ;
            break;
        case OIDVECTOROID:
            *hashfunc = oidvectorhashfast;
            *fasteqfunc = oidvectoreqfast;
            *eqfunc = F_OIDVECTOREQ;
            break;
        default:
            ereport(FATAL, (errmsg("type %u not supported as catcache key", keytype)));
            *hashfunc = NULL; /* keep compiler quiet */

            *eqfunc = InvalidOid;
            break;
    }
}

/*
 *		CatalogCacheComputeHashValue
 *
 * Compute the hash value associated with a given set of lookup keys
 */
uint32 CatalogCacheComputeHashValue(CCHashFN *cc_hashfunc, int nkeys, Datum *arguments)
{
    uint32 hashValue = 0;
    uint32 oneHash;

    switch (nkeys) {
        case 4:
            oneHash = (cc_hashfunc[3])(arguments[3]);
            hashValue ^= oneHash << 24;
            hashValue ^= oneHash >> 8;
            /* FALLTHROUGH */
        case 3:
            oneHash = (cc_hashfunc[2])(arguments[2]);
            hashValue ^= oneHash << 16;
            hashValue ^= oneHash >> 16;
            /* FALLTHROUGH */
        case 2:
            oneHash = (cc_hashfunc[1])(arguments[1]);
            hashValue ^= oneHash << 8;
            hashValue ^= oneHash >> 24;
            /* FALLTHROUGH */
        case 1:
            oneHash = (cc_hashfunc[0])(arguments[0]);
            hashValue ^= oneHash;
            break;
        default:
            ereport(FATAL, (errmsg("wrong number of hash keys: %d", nkeys)));
            break;
    }

    return hashValue;
}

uint32 CatalogCacheComputeTupleHashValueInternal(
    int* cc_keyno, TupleDesc cc_tupdesc, CCHashFN *cc_hashfunc, Oid cc_reloid, int nkeys, HeapTuple tuple)
{
    Datum v1 = 0, v2 = 0, v3 = 0, v4 = 0;
    bool isNull = false;

    /* Now extract key fields from tuple, insert into scankey */
    switch (nkeys) {
        case 4:
            v4 = (cc_keyno[3] == ObjectIdAttributeNumber) ? ObjectIdGetDatum(HeapTupleGetOid(tuple))
                                                          : fastgetattr(tuple, cc_keyno[3], cc_tupdesc, &isNull);
#ifndef ENABLE_MULTIPLE_NODES
            Assert(!isNull || ((cc_reloid == ProcedureRelationId) && v4 == 0));
#else
            Assert(!isNull);
#endif
        case 3:
            v3 = (cc_keyno[2] == ObjectIdAttributeNumber) ? ObjectIdGetDatum(HeapTupleGetOid(tuple))
                                                          : fastgetattr(tuple, cc_keyno[2], cc_tupdesc, &isNull);
            Assert(!isNull);
            /* FALLTHROUGH */
        case 2:
            v2 = (cc_keyno[1] == ObjectIdAttributeNumber) ? ObjectIdGetDatum(HeapTupleGetOid(tuple))
                                                          : fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &isNull);
#ifndef ENABLE_MULTIPLE_NODES
            Assert(!isNull || ((cc_reloid == ProcedureRelationId) && v2 == 0));
#else
            Assert(!isNull);
#endif
            /* FALLTHROUGH */
        case 1:
            v1 = (cc_keyno[0] == ObjectIdAttributeNumber) ? ObjectIdGetDatum(HeapTupleGetOid(tuple))
                                                          : fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &isNull);
            Assert(!isNull);
            break;
        default:
            ereport(FATAL, (errmsg("wrong number of hash keys: %d", nkeys)));
            break;
    }

    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;
    return CatalogCacheComputeHashValue(cc_hashfunc, nkeys, arguments);
}

/*
 *		CatalogCacheComputeTupleHashValue
 *
 * Compute the hash value associated with a given tuple to be cached
 */
uint32 CatalogCacheComputeTupleHashValue(
    int cc_id, int* cc_keyno, TupleDesc cc_tupdesc, CCHashFN *cc_hashfunc, Oid cc_reloid, int cc_nkeys, HeapTuple tup)
{
    uint32 hashValue;

    switch (cc_id) {
#ifndef ENABLE_MULTIPLE_NODES
        case PROCALLARGS: {
            hashValue = CatalogCacheComputeTupleHashValueForProcAllArgs(
                cc_keyno, cc_tupdesc, cc_hashfunc, cc_reloid, cc_nkeys, tup);
            break;
        }
#endif
        default:
            hashValue = CatalogCacheComputeTupleHashValueInternal(
                cc_keyno, cc_tupdesc, cc_hashfunc, cc_reloid, cc_nkeys, tup);
            break;
    }

    return hashValue;
}

/*
 *		CatalogCacheCompareTuple
 *
 * Compare a tuple to the passed arguments.
 */
bool CatalogCacheCompareTuple(
    const CCFastEqualFN *cc_fastequal, int nkeys, const Datum* cachekeys, const Datum* searchkeys)
{
    int i;

    for (i = 0; i < nkeys; i++) {
        if (!(cc_fastequal[i])(cachekeys[i], searchkeys[i]))
            return false;
    }
    return true;
}

#ifdef CATCACHE_STATS

static void CatCachePrintStats(int code, Datum arg)
{
    CatCache* cache = NULL;
    long cc_searches = 0;
    long cc_hits = 0;
    long cc_neg_hits = 0;
    long cc_newloads = 0;
    long cc_invals = 0;
    long cc_lsearches = 0;
    long cc_lhits = 0;

    for (cache = u_sess->cache_cxt.cache_header->ch_caches; cache; cache = cache->cc_next) {
        if (cache->cc_ntup == 0 && cache->cc_searches == 0)
            continue; /* don't print unused caches */
        ereport(DEBUG2,
            (errmsg("catcache %s/%u: %d tup, %ld srch, %ld+%ld=%ld hits, %ld+%ld=%ld loads, %ld invals, %ld lsrch, %ld "
                    "lhits",
                cache->cc_relname,
                cache->cc_indexoid,
                cache->cc_ntup,
                cache->cc_searches,
                cache->cc_hits,
                cache->cc_neg_hits,
                cache->cc_hits + cache->cc_neg_hits,
                cache->cc_newloads,
                cache->cc_searches - cache->cc_hits - cache->cc_neg_hits - cache->cc_newloads,
                cache->cc_searches - cache->cc_hits - cache->cc_neg_hits,
                cache->cc_invals,
                cache->cc_lsearches,
                cache->cc_lhits)));
        cc_searches += cache->cc_searches;
        cc_hits += cache->cc_hits;
        cc_neg_hits += cache->cc_neg_hits;
        cc_newloads += cache->cc_newloads;
        cc_invals += cache->cc_invals;
        cc_lsearches += cache->cc_lsearches;
        cc_lhits += cache->cc_lhits;
    }
    ereport(DEBUG2,
        (errmsg(
            "catcache totals: %d tup, %ld srch, %ld+%ld=%ld hits, %ld+%ld=%ld loads, %ld invals, %ld lsrch, %ld lhits",
            u_sess->cache_cxt.cache_header->ch_ntup,
            cc_searches,
            cc_hits,
            cc_neg_hits,
            cc_hits + cc_neg_hits,
            cc_newloads,
            cc_searches - cc_hits - cc_neg_hits - cc_newloads,
            cc_searches - cc_hits - cc_neg_hits,
            cc_invals,
            cc_lsearches,
            cc_lhits)));
}
#endif /* CATCACHE_STATS */

/*
 *		CatCacheRemoveCTup
 *
 * Unlink and delete the given cache entry
 *
 * NB: if it is a member of a CatCList, the CatCList is deleted too.
 * Both the cache entry and the list had better have zero refcount.
 */
static void CatCacheRemoveCTup(CatCache* cache, CatCTup* ct)
{
    Assert(ct->refcount == 0);
    Assert(ct->my_cache == cache);
    Assert(!ct->isnailed);

    if (ct->c_list) {
        /*
         * The cleanest way to handle this is to call CatCacheRemoveCList,
         * which will recurse back to me, and the recursive call will do the
         * work.  Set the "dead" flag to make sure it does recurse.
         */
        ct->dead = true;
        CatCacheRemoveCList(cache, ct->c_list);
        return; /* nothing left to do */
    }

    /* delink from linked list */
    DLRemove(&ct->cache_elem);

    /*
     * Free keys when we're dealing with a negative entry, normal entries just
     * point into tuple, allocated together with the CatCTup.
     */
    if (ct->negative)
        CatCacheFreeKeys(cache->cc_tupdesc, cache->cc_nkeys, cache->cc_keyno, ct->keys);
    pfree_ext(ct);

    --cache->cc_ntup;
    --u_sess->cache_cxt.cache_header->ch_ntup;
}

/*
 *		CatCacheRemoveCList
 *
 * Unlink and delete the given cache list entry
 *
 * NB: any dead member entries that become unreferenced are deleted too.
 */
static void CatCacheRemoveCList(CatCache* cache, CatCList* cl)
{
    int i;

    Assert(cl->refcount == 0);
    Assert(cl->my_cache == cache);
    Assert(!cl->isnailed);

    /* delink from member tuples */
    for (i = cl->n_members; --i >= 0;) {
        CatCTup* ct = cl->systups[i];

        Assert(ct->c_list == cl);
        ct->c_list = NULL;
        /* if the member is dead and now has no references, remove it */
        if (
#ifndef CATCACHE_FORCE_RELEASE
            ct->dead &&
#endif
            ct->refcount == 0)
            CatCacheRemoveCTup(cache, ct);
    }

    /* delink from linked list */
    DLRemove(&cl->cache_elem);

    /* free associated column data */
    CatCacheFreeKeys(cache->cc_tupdesc, cl->nkeys, cache->cc_keyno, cl->keys);
    pfree_ext(cl);
}

/*
 *	CatalogCacheIdInvalidate
 *
 *	Invalidate entries in the specified cache, given a hash value.
 *
 *	We delete cache entries that match the hash value, whether positive
 *	or negative.  We don't care whether the invalidation is the result
 *	of a tuple insertion or a deletion.
 *
 *	We used to try to match positive cache entries by TID, but that is
 *	unsafe after a VACUUM FULL on a system catalog: an inval event could
 *	be queued before VACUUM FULL, and then processed afterwards, when the
 *	target tuple that has to be invalidated has a different TID than it
 *	did when the event was created.  So now we just compare hash values and
 *	accept the small risk of unnecessary invalidations due to false matches.
 *
 *	This routine is only quasi-public: it should only be used by inval.c.
 */
void CatalogCacheIdInvalidate(int cacheId, uint32 hashValue)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->systabcache.CacheIdHashValueInvalidateLocal(cacheId, hashValue);
        return;
    }
    CatCache* ccp = NULL;

    CACHE3_elog(DEBUG2, "CatalogCacheIdInvalidate: called, cacheId %d, hashValue: %d", cacheId, hashValue);

    /*
     * inspect caches to find the proper cache
     */
    for (ccp = u_sess->cache_cxt.cache_header->ch_caches; ccp; ccp = ccp->cc_next) {
        Index hashIndex;
        Dlelem* elt = NULL;
        Dlelem* nextelt = NULL;

        if (cacheId != ccp->id)
            continue;

        /*
         * We don't bother to check whether the cache has finished
         * initialization yet; if not, there will be no entries in it so no
         * problem.
         */

        /*
         * Invalidate *all* CatCLists in this cache; it's too hard to tell
         * which searches might still be correct, so just zap 'em all.
         */
        for (elt = DLGetHead(&ccp->cc_lists); elt; elt = nextelt) {
            CatCList* cl = (CatCList*)DLE_VAL(elt);

            nextelt = DLGetSucc(elt);

            if (cl->isnailed)
                continue;

            if (cl->refcount > 0)
                cl->dead = true;
            else
                CatCacheRemoveCList(ccp, cl);
        }

        /*
         * inspect the proper hash bucket for tuple matches
         */
        hashIndex = HASH_INDEX(hashValue, ccp->cc_nbuckets);

        for (elt = DLGetHead(&ccp->cc_bucket[hashIndex]); elt; elt = nextelt) {
            CatCTup* ct = (CatCTup*)DLE_VAL(elt);

            nextelt = DLGetSucc(elt);

            if (hashValue == ct->hash_value) {
                if (ct->isnailed) {
                    continue;
                }
                if (ct->refcount > 0 || (ct->c_list && ct->c_list->refcount > 0)) {
                    ct->dead = true;
                    /* list, if any, was marked dead above */
                    Assert(ct->c_list == NULL || ct->c_list->dead);
                } else
                    CatCacheRemoveCTup(ccp, ct);
                CACHE3_elog(
                    DEBUG2, "CatalogCacheIdInvalidate: cacheId: %d hashValue %d invalidated", cacheId, hashValue);
#ifdef CATCACHE_STATS
                ccp->cc_invals++;
#endif
                /* could be multiple matches, so keep looking! */
            }
        }
        break; /* need only search this one cache */
    }
}

/* ----------------------------------------------------------------
 *					   public functions
 * ----------------------------------------------------------------
 */

/*
 *		AtEOXact_CatCache
 *
 * Clean up catcaches at end of main transaction (either commit or abort)
 *
 * As of PostgreSQL 8.1, catcache pins should get released by the
 * ResourceOwner mechanism.  This routine is just a debugging
 * cross-check that no pins remain.
 */
void AtEOXact_CatCache(bool isCommit)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->systabcache.AtEOXact_CatCache(isCommit);
        return;
    }
#ifdef USE_ASSERT_CHECKING
    if (assert_enabled) {
        CatCache* ccp = NULL;

        for (ccp = u_sess->cache_cxt.cache_header->ch_caches; ccp; ccp = ccp->cc_next) {
            Dlelem* elt = NULL;
            int i;

            /* Check CatCLists */
            for (elt = DLGetHead(&ccp->cc_lists); elt; elt = DLGetSucc(elt)) {
                CatCList* cl = (CatCList*)DLE_VAL(elt);

                Assert(cl->cl_magic == CL_MAGIC);
                Assert(cl->refcount == 0);
                Assert(!cl->dead);
            }

            /* Check individual tuples */
            for (i = 0; i < ccp->cc_nbuckets; i++) {
                for (elt = DLGetHead(&ccp->cc_bucket[i]); elt; elt = DLGetSucc(elt)) {
                    CatCTup* ct = (CatCTup*)DLE_VAL(elt);

                    if (ct == NULL)
                        continue;

                    Assert(ct->ct_magic == CT_MAGIC);
                    Assert(ct->refcount == 0);
                    Assert(!ct->dead);
                }
            }
        }
    }
#endif
}

/*
 *		ResetCatalogCache
 *
 * Reset one catalog cache to empty.
 *
 * This is not very efficient if the target cache is nearly empty.
 * However, it shouldn't need to be efficient; we don't invoke it often.
 */
static void ResetCatalogCache(CatCache* cache)
{
    Dlelem* elt = NULL;
    Dlelem* nextelt = NULL;
    int i;

    /* Remove each list in this cache, or at least mark it dead */
    for (elt = DLGetHead(&cache->cc_lists); elt; elt = nextelt) {
        CatCList* cl = (CatCList*)DLE_VAL(elt);

        nextelt = DLGetSucc(elt);

        if (cl->isnailed)
            continue;

        if (cl->refcount > 0)
            cl->dead = true;
        else
            CatCacheRemoveCList(cache, cl);
    }

    /* Remove each tuple in this cache, or at least mark it dead */
    for (i = 0; i < cache->cc_nbuckets; i++) {
        for (elt = DLGetHead(&cache->cc_bucket[i]); elt; elt = nextelt) {
            CatCTup* ct = (CatCTup*)DLE_VAL(elt);

            nextelt = DLGetSucc(elt);

            if (ct->isnailed)
                continue;

            if (ct->refcount > 0 || (ct->c_list && ct->c_list->refcount > 0)) {
                ct->dead = true;
                /* list, if any, was marked dead above */
                Assert(ct->c_list == NULL || ct->c_list->dead);
            } else
                CatCacheRemoveCTup(cache, ct);
#ifdef CATCACHE_STATS
            cache->cc_invals++;
#endif
        }
    }
}

/*
 *		ResetCatalogCaches
 *
 * Reset all caches when a shared cache inval event forces it
 */
void ResetCatalogCaches(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->systabcache.ResetCatalogCaches();
        return;
    }
    CatCache* cache = NULL;

    if (!RecoveryInProgress()) {
        CACHE1_elog(DEBUG2, "ResetCatalogCaches called");
    }
    for (cache = u_sess->cache_cxt.cache_header->ch_caches; cache; cache = cache->cc_next)
        ResetCatalogCache(cache);

    if (!RecoveryInProgress()) {
        CACHE1_elog(DEBUG2, "end of ResetCatalogCaches call");
    }
}

/*
 *		CatalogCacheFlushCatalog
 *
 *	Flush all catcache entries that came from the specified system catalog.
 *	This is needed after VACUUM FULL/CLUSTER on the catalog, since the
 *	tuples very likely now have different TIDs than before.  (At one point
 *	we also tried to force re-execution of CatalogCacheInitializeCache for
 *	the cache(s) on that catalog.  This is a bad idea since it leads to all
 *	kinds of trouble if a cache flush occurs while loading cache entries.
 *	We now avoid the need to do it by copying cc_tupdesc out of the relcache,
 *	rather than relying on the relcache to keep a tupdesc for us.  Of course
 *	this assumes the tupdesc of a cachable system table will not change...)
 */
void CatalogCacheFlushCatalog(Oid catId)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->systabcache.CatalogCacheFlushCatalogLocal(catId);
        return;
    }
    CatCache* cache = NULL;

    CACHE2_elog(DEBUG2, "CatalogCacheFlushCatalog called for %u", catId);

    for (cache = u_sess->cache_cxt.cache_header->ch_caches; cache; cache = cache->cc_next) {
        /* Does this cache store tuples of the target catalog? */
        if (cache->cc_reloid == catId) {
            /* Yes, so flush all its contents */
            ResetCatalogCache(cache);

            /* Tell inval.c to call syscache callbacks for this cache */
            CallSessionSyscacheCallbacks(cache->id, 0);
        }
    }

    CACHE1_elog(DEBUG2, "end of CatalogCacheFlushCatalog call");
}

/*
 *		InitCatCache
 *
 *	This allocates and initializes a cache for a system catalog relation.
 *	Actually, the cache is only partially initialized to avoid opening the
 *	relation.  The relation will be opened and the rest of the cache
 *	structure initialized on the first access.
 */
#ifdef CACHEDEBUG
#define InitCatCache_DEBUG2                                               \
    do {                                                                  \
        ereport(DEBUG2,                                                   \
            (errmsg("InitCatCache: rel=%u ind=%u id=%d nkeys=%d size=%d", \
                cp->cc_reloid,                                            \
                cp->cc_indexoid,                                          \
                cp->id,                                                   \
                cp->cc_nkeys,                                             \
                cp->cc_nbuckets)));                                       \
    } while (0)
#else
#define InitCatCache_DEBUG2
#endif

CatCache* InitCatCache(int id, Oid reloid, Oid indexoid, int nkeys, const int* key, int nbuckets)
{
    CatCache* cp = NULL;
    MemoryContext oldcxt;
    size_t sz;
    int i;

    /*
     * nbuckets is the number of hash buckets to use in this catcache.
     * Currently we just use a hard-wired estimate of an appropriate size for
     * each cache; maybe later make them dynamically resizable?
     *
     * nbuckets must be a power of two.  We check this via Assert rather than
     * a full runtime check because the values will be coming from constant
     * tables.
     *
     * If you're confused by the power-of-two check, see comments in
     * bitmapset.c for an explanation.
     */
    Assert(nbuckets > 0 && (nbuckets & -nbuckets) == nbuckets);

    /*
     * first switch to the cache context so our allocations do not vanish at
     * the end of a transaction
     */
    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

    /*
     * if first time through, initialize the cache group header
     */
    if (u_sess->cache_cxt.cache_header == NULL) {
        u_sess->cache_cxt.cache_header = (CatCacheHeader*)palloc(sizeof(CatCacheHeader));
        u_sess->cache_cxt.cache_header->ch_caches = NULL;
        u_sess->cache_cxt.cache_header->ch_ntup = 0;
#ifdef CATCACHE_STATS
        /* set up to dump stats at backend exit */
        on_proc_exit(CatCachePrintStats, 0);
#endif
    }

    /*
     * Allocate a new cache structure, aligning to a cacheline boundary
     *
     * Note: we assume zeroing initializes the Dllist headers correctly
     */
    sz = offsetof(CatCache, cc_bucket) + (nbuckets + 1) * sizeof(Dllist) + PG_CACHE_LINE_SIZE;
    cp = (CatCache*)CACHELINEALIGN(palloc0(sz));

    /*
     * initialize the cache's relation information for the relation
     * corresponding to this cache, and initialize some of the new cache's
     * other internal fields.  But don't open the relation yet.
     */
    cp->id = id;
    cp->cc_relname = "(not known yet)";
    cp->cc_reloid = reloid;
    cp->cc_indexoid = indexoid;
    cp->cc_relisshared = false; /* temporary */
    cp->cc_tupdesc = (TupleDesc)NULL;
    cp->cc_ntup = 0;
    cp->cc_nbuckets = nbuckets;
    cp->cc_nkeys = nkeys;
    for (i = 0; i < nkeys; ++i)
        cp->cc_keyno[i] = key[i];

    /*
     * new cache is initialized as far as we can go for now. print some
     * debugging information, if appropriate.
     */
    InitCatCache_DEBUG2;

    /*
     * add completed cache to top of group header's list
     */
    cp->cc_next = u_sess->cache_cxt.cache_header->ch_caches;
    u_sess->cache_cxt.cache_header->ch_caches = cp;

    /*
     * back to the old context before we return...
     */
    MemoryContextSwitchTo(oldcxt);

    return cp;
}

/*
 *		CatalogCacheInitializeCache
 *
 * This function does final initialization of a catcache: obtain the tuple
 * descriptor and set up the hash and equality function links.	We assume
 * that the relcache entry can be opened at this point!
 */
#ifdef CACHEDEBUG
#define CatalogCacheInitializeCache_DEBUG1 \
    ereport(DEBUG2, (errmsg("CatalogCacheInitializeCache: cache @%p rel=%u", cache, cache->cc_reloid)))

#define CatalogCacheInitializeCache_DEBUG2                                                                             \
    do {                                                                                                               \
        if (cache->cc_keyno[i] > 0) {                                                                                  \
            ereport(DEBUG2,                                                                                            \
                (errmsg("CatalogCacheInitializeCache: load %d/%d w/%d, %u",                                            \
                    i + 1,                                                                                             \
                    cache->cc_nkeys,                                                                                   \
                    cache->cc_keyno[i],                                                                                \
                    tupdesc->attrs[cache->cc_keyno[i] - 1].atttypid)));                                               \
        } else {                                                                                                       \
            ereport(DEBUG2,                                                                                            \
                (errmsg("CatalogCacheInitializeCache: load %d/%d w/%d", i + 1, cache->cc_nkeys, cache->cc_keyno[i]))); \
        }                                                                                                              \
    } while (0)
#else
#define CatalogCacheInitializeCache_DEBUG1
#define CatalogCacheInitializeCache_DEBUG2
#endif

static void CatalogCacheInitializeCache(CatCache* cache)
{
    Relation relation;
    MemoryContext oldcxt;
    TupleDesc tupdesc;
    int i;

    CatalogCacheInitializeCache_DEBUG1;

    /*
     * During inplace or online upgrade, the to-be-fabricated catalogs are still missing,
     * for which we can not throw an ERROR.
     */
    LockRelationOid(cache->cc_reloid, AccessShareLock);

    relation = RelationIdGetRelation(cache->cc_reloid);

    if (!RelationIsValid(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not open relation with OID %u", cache->cc_reloid)));
    }

    pgstat_initstats(relation);

    /*
     * switch to the cache context so our allocations do not vanish at the end
     * of a transaction
     */
    Assert(u_sess->cache_mem_cxt != NULL);

    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

    /*
     * copy the relcache's tuple descriptor to permanent cache storage
     */
    tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

    /*
     * save the relation's name and relisshared flag, too (cc_relname is used
     * only for debugging purposes)
     */
    cache->cc_relname = pstrdup(RelationGetRelationName(relation));
    cache->cc_relisshared = RelationGetForm(relation)->relisshared;

    /*
     * return to the caller's memory context and close the rel
     */
    MemoryContextSwitchTo(oldcxt);

    heap_close(relation, AccessShareLock);

    CACHE3_elog(DEBUG2, "CatalogCacheInitializeCache: %s, %d keys", cache->cc_relname, cache->cc_nkeys);

    /*
     * initialize cache's key information
     */
    for (i = 0; i < cache->cc_nkeys; ++i) {
        Oid keytype;
        RegProcedure eqfunc;

        CatalogCacheInitializeCache_DEBUG2;

        if (cache->cc_keyno[i] > 0)
            keytype = tupdesc->attrs[cache->cc_keyno[i] - 1].atttypid;
        else {
            if (cache->cc_keyno[i] != ObjectIdAttributeNumber)
                ereport(FATAL, (errmsg("only sys attr supported in caches is OID")));
            keytype = OIDOID;
        }

        if (u_sess->hook_cxt.pluginCCHashEqFuncs == NULL ||
            !((pluginCCHashEqFuncs)(u_sess->hook_cxt.pluginCCHashEqFuncs))(keytype, &cache->cc_hashfunc[i], &eqfunc,
                &cache->cc_fastequal[i], cache->id)) {
            GetCCHashEqFuncs(keytype, &cache->cc_hashfunc[i], &eqfunc, &cache->cc_fastequal[i]);
        }

        /*
         * Do equality-function lookup (we assume this won't need a catalog
         * lookup for any supported type)
         */
        fmgr_info_cxt(eqfunc, &cache->cc_skey[i].sk_func, u_sess->cache_mem_cxt);

        /* Initialize sk_attno suitably for HeapKeyTest() and heap scans */
        cache->cc_skey[i].sk_attno = cache->cc_keyno[i];

        /* Fill in sk_strategy as well --- always standard equality */
        cache->cc_skey[i].sk_strategy = BTEqualStrategyNumber;
        cache->cc_skey[i].sk_subtype = InvalidOid;
        /* Currently, there are no catcaches on collation-aware data types */
        if (keytype == TEXTOID) {
            cache->cc_skey[i].sk_collation = DEFAULT_COLLATION_OID;
        } else {
            cache->cc_skey[i].sk_collation = InvalidOid;
        }
    }

    /*
     * mark this cache fully initialized
     */
    cache->cc_tupdesc = tupdesc;
}

/*
 * InitCatCachePhase2 -- external interface for CatalogCacheInitializeCache
 *
 * One reason to call this routine is to ensure that the relcache has
 * created entries for all the catalogs and indexes referenced by catcaches.
 * Therefore, provide an option to open the index as well as fixing the
 * cache itself.  An exception is the indexes on pg_am, which we don't use
 * (cf. IndexScanOK).
 */
void InitCatCachePhase2(CatCache* cache, bool touch_index)
{
    if (cache->cc_tupdesc == NULL)
        CatalogCacheInitializeCache(cache);
    /*
     * If the relcache of the underneath catalog has not been built,
     * nor can that of its index.
     */
    if (touch_index && cache->cc_tupdesc != NULL && cache->id != AMOID && cache->id != AMNAME) {
        Relation idesc;

        /*
         * We must lock the underlying catalog before opening the index to
         * avoid deadlock, since index_open could possibly result in reading
         * this same catalog, and if anyone else is exclusive-locking this
         * catalog and index they'll be doing it in that order.
         */
        LockRelationOid(cache->cc_reloid, AccessShareLock);
        LockRelationOid(cache->cc_indexoid, AccessShareLock);

        idesc = RelationIdGetRelation(cache->cc_indexoid);
#ifndef ENABLE_MULTIPLE_NODES
        if (!(t_thrd.proc->workingVersionNum < 92470 && cache->cc_indexoid == ProcedureNameAllArgsNspIndexId)) {
            if (!RelationIsValid(idesc)) {
                elog(LOG, "CatalogCacheCompute:%d", t_thrd.proc->workingVersionNum);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not open index with OID %u", cache->cc_indexoid)));
            }
        } else {
            UnlockRelationOid(cache->cc_reloid, AccessShareLock);
            UnlockRelationOid(cache->cc_indexoid, AccessShareLock);
            return;
        }
#else
        if (!RelationIsValid(idesc)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not open index with OID %u", cache->cc_indexoid)));
        }
#endif
        index_close(idesc, AccessShareLock);
        UnlockRelationOid(cache->cc_reloid, AccessShareLock);
    }
}

/*
 *		IndexScanOK
 *
 *		This function checks for tuples that will be fetched by
 *		IndexSupportInitialize() during relcache initialization for
 *		certain system indexes that support critical syscaches.
 *		We can't use an indexscan to fetch these, else we'll get into
 *		infinite recursion.  A plain heap scan will work, however.
 *		Once we have completed relcache initialization (signaled by
 *		u_sess->relcache_cxt.criticalRelcachesBuilt), we don't have to worry anymore.
 *
 *		Similarly, during backend startup we have to be able to use the
 *		pg_authid and pg_auth_members syscaches for authentication even if
 *		we don't yet have relcache entries for those catalogs' indexes.
 */
bool IndexScanOK(int cache_id)
{
    if (SS_STANDBY_MODE) {
        return false;
    }

    switch (cache_id) {
        case INDEXRELID:

            /*
             * Rather than tracking exactly which indexes have to be loaded
             * before we can use indexscans (which changes from time to time),
             * just force all pg_index searches to be heap scans until we've
             * built the critical relcaches.
             */
            if (!LocalRelCacheCriticalRelcachesBuilt())
                return false;
            break;

        case AMOID:
        case AMNAME:

            /*
             * Always do heap scans in pg_am, because it's so small there's
             * not much point in an indexscan anyway.  We *must* do this when
             * initially building critical relcache entries, but we might as
             * well just always do it.
             */
            return false;

        case AUTHNAME:
        case AUTHOID:
        case AUTHMEMMEMROLE:
        case USERSTATUSROLEID:

            /*
             * Protect authentication lookups occurring before relcache has
             * collected entries for shared indexes.
             */
            if (!LocalRelCacheCriticalSharedRelcachesBuilt())
                return false;
            break;
        default:
            break;
    }

    /* Normal case, allow index scan */
    return true;
}

/*
 *	SearchCatCacheInternal
 *
 *		This call searches a system cache for a tuple, opening the relation
 *		if necessary (on the first access to a particular cache).
 *
 *		The result is NULL if not found, or a pointer to a HeapTuple in
 *		the cache.	The caller must not modify the tuple, and must call
 *		ReleaseCatCache() when done with it.
 *
 * The search key values should be expressed as Datums of the key columns'
 * datatype(s).  (Pass zeroes for any unused parameters.)  As a special
 * exception, the passed-in key for a NAME column can be just a C string;
 * the caller need not go to the trouble of converting it to a fully
 * null-padded NAME.
 */

HeapTuple SearchCatCache(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4, int level)
{
    return SearchCatCacheInternal(cache, cache->cc_nkeys, v1, v2, v3, v4, level);
}

/*
 * SearchCatCacheN() are SearchCatCache() versions for a specific number of
 * arguments. The compiler can inline the body and unroll loops, making them a
 * bit faster than SearchCatCache().
 */
HeapTuple SearchCatCache1(CatCache* cache, Datum v1)
{
    return SearchCatCacheInternal(cache, 1, v1, 0, 0, 0);
}

HeapTuple SearchCatCache2(CatCache* cache, Datum v1, Datum v2)
{
    return SearchCatCacheInternal(cache, 2, v1, v2, 0, 0);
}

HeapTuple SearchCatCache3(CatCache* cache, Datum v1, Datum v2, Datum v3)
{
    return SearchCatCacheInternal(cache, 3, v1, v2, v3, 0);
}

HeapTuple SearchCatCache4(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4)
{
    return SearchCatCacheInternal(cache, 4, v1, v2, v3, v4);
}

void SearchCatCacheCheck(){
    if (IsAbortedTransactionBlockState()) {
        force_backtrace_messages = true;
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("SearchCatCacheCheck:current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar)));
    }
    return;
}

/*
 * Work-horse for SearchCatCache/SearchCatCacheN.
 */
HeapTuple SearchCatCacheInternal(CatCache* cache, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level)
{
    /* dont call SearchCatCache , call SearchSysCache instead */
    Assert(!EnableLocalSysCache());
    Datum arguments[CATCACHE_MAXKEYS];
    uint32 hashValue;
    Index hashIndex;
    Dlelem* elt = NULL;
    CatCTup* ct = NULL;

    Assert(cache->cc_nkeys == nkeys);

    SearchCatCacheCheck();

    /*
     * one-time startup overhead for each cache
     */
    if (unlikely(cache->cc_tupdesc == NULL))
        CatalogCacheInitializeCache(cache);

#ifdef CATCACHE_STATS
    cache->cc_searches++;
#endif

    /* Initialize local parameter array */
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    /*
     * find the hash bucket in which to look for the tuple
     */
    hashValue = CatalogCacheComputeHashValue(cache->cc_hashfunc, nkeys, arguments);
    hashIndex = HASH_INDEX(hashValue, (uint32)cache->cc_nbuckets);

    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     */
    for (elt = DLGetHead(&cache->cc_bucket[hashIndex]); elt; elt = DLGetSucc(elt)) {

        ct = (CatCTup*)DLE_VAL(elt);

        if (ct->dead)
            continue; /* ignore dead entries */

        if (ct->hash_value != hashValue)
            continue; /* quickly skip entry if wrong hash val */

        if (!CatalogCacheCompareTuple(cache->cc_fastequal, nkeys, ct->keys, arguments))
            continue;

        /*
         * We found a match in the cache.  Move it to the front of the list
         * for its hashbucket, in order to speed subsequent searches.  (The
         * most frequently accessed elements in any hashbucket will tend to be
         * near the front of the hashbucket's list.)
         */
        DLMoveToFront(&ct->cache_elem);

        /*
         * If it's a positive entry, bump its refcount and return it. If it's
         * negative, we can report failure to the caller.
         */
        if (!ct->negative && t_thrd.utils_cxt.CurrentResourceOwner != NULL) {
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);

            CACHE3_elog(DEBUG2, "SearchCatCache(%s): found in bucket %d", cache->cc_relname, hashIndex);

#ifdef CATCACHE_STATS
            cache->cc_hits++;
#endif

            return &ct->tuple;
        } else {
            CACHE3_elog(DEBUG2, "SearchCatCache(%s): found neg entry in bucket %d", cache->cc_relname, hashIndex);

#ifdef CATCACHE_STATS
            cache->cc_neg_hits++;
#endif

            return NULL;
        }
    }
    return SearchCatCacheMiss(cache, nkeys, hashValue, hashIndex, v1, v2, v3, v4, level);
}

HeapTuple CreateHeapTuple4BuiltinFuncDesc(const Builtin_func* func, TupleDesc desc);
HeapTuple CreateHeapTuple4BuiltinFunc(const Builtin_func* func, TupleDesc desc);

/*
 * SearchBuiltinProcByNameArgNsp
 *
 *  This function is used to find the identified funtion by namespace, argument type and function name.
 *  Because these three arguments below is the unique index for builtin functions, so we can find only
 *  one buitin function if it exists. Then we create the heap tuple by the infomation we get from builtin
 *  function arrarys,and return it.
 */
static HeapTuple SearchBuiltinProcByNameArgNsp(int nkeys, Datum* arguments)
{
    char* funcname = NULL;
    oidvector* argtypes = NULL;
    const FuncGroup* gfuncs = NULL;

    Assert(nkeys == 3 || nkeys == 4);

    funcname = NameStr(*(DatumGetName(arguments[0])));
    gfuncs = SearchBuiltinFuncByName(funcname);

    if (gfuncs == NULL) {
        CACHE3_elog(DEBUG2, "%s: the function \"%s\" does not in built-in list", __FUNCTION__, funcname);
        return NULL;
    }

    const Builtin_func* bfunc = NULL;
    int argtype_count;
    for (int i = 0; i < gfuncs->fnums; i++) {
        bfunc = &gfuncs->funcs[i];

        // compare namespaceOid
        if (DatumGetObjectId(arguments[2]) != bfunc->pronamespace) {
            continue;
        }

        // compare number of argtypes
        argtypes = (oidvector*)DatumGetArrayTypeP(arguments[1]);
        argtype_count = bfunc->proargtypes.count;
        if (argtype_count != ARR_DIMS(argtypes)[0]) {
            continue;
        }

        // compare all elements of argtypes
        if (argtype_count != 0 &&
            memcmp(argtypes->values, bfunc->proargtypes.values, sizeof(Oid) * argtype_count) != 0) {
            continue;
        }

        CACHE4_elog(DEBUG2,
            "%s: the function \"%s\" is found in built-in list with oid = %u",
            __FUNCTION__,
            funcname,
            bfunc->foid);

        return CreateHeapTuple4BuiltinFunc(bfunc, NULL);
    }

    CACHE4_elog(DEBUG2,
        "%s: \"%s\"'s arguments and namespace(%u) are not matched in built-in list",
        __FUNCTION__,
        funcname,
        DatumGetObjectId(arguments[2]));
    return NULL;
}

/*
 * SearchBuiltinProcByOid
 *
 *  This function is used to find the identified funtion by builtin function's oid. Because function
 *  oid is the unique index for builtin functions, so we can find only one buitin function if it exists.
 *  Then we create the heap tuple by the infomation we get from builtin function arrarys,and return it.
 */
static HeapTuple SearchBuiltinProcByOid(int nkeys, Datum* arguments)
{
    const Builtin_func* bfunc = NULL;

    Assert(nkeys == 1);

    Oid funcoid = DatumGetObjectId(arguments[0]);
    bfunc = SearchBuiltinFuncByOid(funcoid);

    if (bfunc != NULL) {
        CACHE3_elog(DEBUG2, "%s: function Oid (%u) is found in built-in list", __FUNCTION__, bfunc->foid);
        return CreateHeapTuple4BuiltinFunc(bfunc, NULL);
    }

    CACHE3_elog(DEBUG2, "%s: function Oid (%u) is not found in built-in list", __FUNCTION__, bfunc->foid);
    return NULL;
}

/*
 * SearchBuiltinProcCacheMiss
 *
 *  This function is the entry of finding the builtin function by some specific arguments.
 *  Now we have two methods to get the unique builtin function. One is finding by name,
 *  argument types and namespace. Another is finding by function oid. Both of them is the unique
 *  index for builtin functions. We can get the search mode infomation from cache to determine which
 *  function will be called for processing
 */
HeapTuple SearchBuiltinProcCacheMiss(int cache_id, int nkeys, Datum* arguments)
{
    if (CacheIsProcNameArgNsp(cache_id)) {
        return SearchBuiltinProcByNameArgNsp(nkeys, arguments);
    } else if (CacheIsProcOid(cache_id)) {
        return SearchBuiltinProcByOid(nkeys, arguments);
    } else {
        return NULL;
    }
}

HeapTuple GetPgAttributeAttrTuple(TupleDesc tupleDesc, const Form_pg_attribute attr)
{
    Datum values[Natts_pg_attribute];
    bool isnull[Natts_pg_attribute];

    errno_t rc;
    rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "", "");
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");

    values[Anum_pg_attribute_attrelid - 1] = ObjectIdGetDatum(attr->attrelid);
    values[Anum_pg_attribute_attname - 1] = NameGetDatum(&(attr->attname));
    values[Anum_pg_attribute_atttypid - 1] = ObjectIdGetDatum(attr->atttypid);
    values[Anum_pg_attribute_attstattarget - 1] = Int32GetDatum(attr->attstattarget);
    values[Anum_pg_attribute_attlen - 1] = Int16GetDatum(attr->attlen);
    values[Anum_pg_attribute_attnum - 1] = Int16GetDatum(attr->attnum);
    values[Anum_pg_attribute_attndims - 1] = Int32GetDatum(attr->attndims);
    values[Anum_pg_attribute_attcacheoff - 1] = Int32GetDatum(attr->attcacheoff);
    values[Anum_pg_attribute_atttypmod - 1] = Int32GetDatum(attr->atttypmod);
    values[Anum_pg_attribute_attbyval - 1] = BoolGetDatum(attr->attbyval);
    values[Anum_pg_attribute_attstorage - 1] = CharGetDatum(attr->attstorage);
    values[Anum_pg_attribute_attalign - 1] = CharGetDatum(attr->attalign);
    values[Anum_pg_attribute_attnotnull - 1] = BoolGetDatum(attr->attnotnull);
    values[Anum_pg_attribute_atthasdef - 1] = BoolGetDatum(attr->atthasdef);
    values[Anum_pg_attribute_attisdropped - 1] = BoolGetDatum(attr->attisdropped);
    values[Anum_pg_attribute_attislocal - 1] = BoolGetDatum(attr->attislocal);
    values[Anum_pg_attribute_attcmprmode - 1] = Int8GetDatum(attr->attcmprmode);
    values[Anum_pg_attribute_attinhcount - 1] = Int32GetDatum(attr->attinhcount);
    values[Anum_pg_attribute_attcollation - 1] = ObjectIdGetDatum(attr->attcollation);
    values[Anum_pg_attribute_attkvtype - 1] = Int8GetDatum(attr->attkvtype);
    values[Anum_pg_attribute_attdroppedname - 1] = NameGetDatum(&(attr->attdroppedname));

    /* start out with empty permissions and empty options */
    isnull[Anum_pg_attribute_attacl - 1] = true;
    isnull[Anum_pg_attribute_attoptions - 1] = true;
    isnull[Anum_pg_attribute_attfdwoptions - 1] = true;

    /* at default, new fileld attinitdefval of pg_attribute is null. */
    isnull[Anum_pg_attribute_attinitdefval - 1] = true;

    return heap_form_tuple(tupleDesc, values, isnull);
}

HeapTuple SearchPgAttributeCacheMiss(int cache_id, TupleDesc cc_tupdesc, int nkeys, const Datum* arguments)
{
    Assert(nkeys == 2);
    Oid relOid = DatumGetObjectId(arguments[0]);
    CatalogRelationBuildParam catalogDesc = GetCatalogParam(relOid);
    if (catalogDesc.oid == InvalidOid) {
        return NULL;
    }
    const FormData_pg_attribute* catlogAttrs = catalogDesc.attrs;
    FormData_pg_attribute tempAttr;
    if (cache_id == ATTNUM) {
        int16 attNum = DatumGetInt16(arguments[1]);
        Form_pg_attribute attr;
        if (attNum < 0) {
            /* The system table does not have the bucket column or uids column, so incoming false */
            if ((attNum == ObjectIdAttributeNumber && !catalogDesc.hasoids) || attNum == BucketIdAttributeNumber ||
                attNum == UidAttributeNumber) {
                return NULL;
            }
            attr = SystemAttributeDefinition(attNum, catalogDesc.hasoids, false, false);
            attr->attrelid = relOid;
        } else if (attNum <= catalogDesc.natts && attNum > 0) {
            tempAttr = catlogAttrs[attNum - 1];
            attr = &tempAttr;
        } else {
            return NULL;
        }
        return GetPgAttributeAttrTuple(cc_tupdesc, attr);
    } else if (cache_id == ATTNAME) {
        Form_pg_attribute attr;
        for (int16 attnum = 0; attnum < catalogDesc.natts; attnum++) {
            tempAttr = catlogAttrs[attnum];
            attr = &tempAttr;
            if (strcmp(NameStr(*DatumGetName(arguments[1])), NameStr(attr->attname)) == 0) {
                return GetPgAttributeAttrTuple(cc_tupdesc, attr);
            }
        }
        attr = SystemAttributeByName(NameStr(*DatumGetName(arguments[1])), catalogDesc.hasoids);
        if (attr == NULL) {
            return NULL;
        }
        attr->attrelid = relOid;
        return GetPgAttributeAttrTuple(cc_tupdesc, attr);
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("pg_attribute does not have syscache with id %d", cache_id)));
    }
    return NULL;
}

/*
 * Search the actual catalogs, rather than the cache.
 *
 * This is kept separate from SearchCatCacheInternal() to keep the fast-path
 * as small as possible.  To avoid that effort being undone by a helpful
 * compiler, try to explicitly forbid inlining.
 */
static HeapTuple SearchCatCacheMiss(
    CatCache* cache, int nkeys, uint32 hashValue, Index hashIndex, Datum v1, Datum v2, Datum v3, Datum v4, int level)
{
    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    Relation relation;
    SysScanDesc scandesc = NULL;
    HeapTuple ntp;
    CatCTup* ct = NULL;
    Datum arguments[CATCACHE_MAXKEYS];
    errno_t rc = EOK;

    /* Initialize local parameter array */
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    /*
     * Ok, need to make a lookup in the relation, copy the scankey and fill
     * out any per-call fields.
     */
    rc = memcpy_s(cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cache->cc_skey, sizeof(ScanKeyData) * nkeys);
    securec_check(rc, "", "");
    cur_skey[0].sk_argument = v1;
    cur_skey[1].sk_argument = v2;
    cur_skey[2].sk_argument = v3;
    cur_skey[3].sk_argument = v4;

    /* For search a function, we firstly try to search it in built-in function list */
    if (IsProcCache(cache->cc_reloid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        CACHE2_elog(DEBUG2, "SearchCatCacheMiss(%d): function not found in pg_proc", cache->id);

        ntp = SearchBuiltinProcCacheMiss(cache->id, nkeys, arguments);
        if (HeapTupleIsValid(ntp)) {
            CACHE2_elog(DEBUG2, "SearchCatCacheMiss(%d): match a built-in function", cache->id);
            ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
            heap_freetuple(ntp);
            /* immediately set the refcount to 1 */
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        }
    }

    /* Insert hardcoded system catalogs' attributes into pg_attribute's syscache. */
    if (IsAttributeCache(cache->cc_reloid) && IsSystemObjOid(DatumGetObjectId(arguments[0]))) {
        CACHE2_elog(DEBUG2, "SearchCatCacheMiss: cat tuple not in cat cache %d", cache->id);
        ntp = SearchPgAttributeCacheMiss(cache->id, cache->cc_tupdesc, nkeys, arguments);
        if (HeapTupleIsValid(ntp)) {
            ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
            heap_freetuple(ntp);
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        }
    }

    /*
     * Tuple was not found in cache, so we have to try to retrieve it directly
     * from the relation.  If found, we will add it to the cache; if not
     * found, we will add a negative cache entry instead.
     *
     * NOTE: it is possible for recursive cache lookups to occur while reading
     * the relation --- for example, due to shared-cache-inval messages being
     * processed during heap_open().  This is OK.  It's even possible for one
     * of those lookups to find and enter the very same tuple we are trying to
     * fetch here.	If that happens, we will enter a second copy of the tuple
     * into the cache.	The first copy will never be referenced again, and
     * will eventually age out of the cache, so there's no functional problem.
     * This case is rare enough that it's not worth expending extra cycles to
     * detect.
     */
    if (ct == NULL) {
        relation = heap_open(cache->cc_reloid, AccessShareLock);

        ereport(DEBUG1, (errmsg("cache->cc_reloid - %d", cache->cc_reloid)));

        if (u_sess->hook_cxt.pluginSearchCatHook != NULL) {
            if (HeapTupleIsValid(ntp = ((searchCatFunc)(u_sess->hook_cxt.pluginSearchCatHook))(relation,
                cache->cc_indexoid, cache->id, nkeys, cur_skey, &scandesc))) {
                ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
                /* immediately set the refcount to 1 */
                ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
                ct->refcount++;
                ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
            }
        } else {
            scandesc = systable_beginscan(
                relation, cache->cc_indexoid, IndexScanOK(cache->id), NULL, nkeys, cur_skey);

            while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
                ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
                /* immediately set the refcount to 1 */
                ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
                ct->refcount++;
                ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
                break; /* assume only one match */
            }
        }

        systable_endscan(scandesc);

        heap_close(relation, AccessShareLock);
    }

    /*
     * If tuple was not found, we need to build a negative cache entry
     * containing a fake tuple.  The fake tuple has the correct key columns,
     * but nulls everywhere else.
     *
     * In bootstrap mode, we don't build negative entries, because the cache
     * invalidation mechanism isn't alive and can't clear them if the tuple
     * gets created later.	(Bootstrap doesn't do UPDATEs, so it doesn't need
     * cache inval for that.)
     */
    if (ct == NULL) {
        if (IsBootstrapProcessingMode())
            return NULL;
#ifdef USE_SPQ
        if ((cache->id == ATTNUM && DatumGetInt16(arguments[1]) == RootSelfItemPointerAttributeNumber) ||
            (cache->id == ATTNAME && (strcmp(DatumGetCString(arguments[1]), "_root_ctid") == 0))) {
            ct = CatalogCacheCreateEntry(cache, NULL, arguments, hashValue, hashIndex, false);

            /* immediately set the refcount to 1 */
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        } else
#endif
        {
        ct = CatalogCacheCreateEntry(cache, NULL, arguments, hashValue, hashIndex, true);

        CACHE4_elog(DEBUG2,
            "SearchCatCache(%s): Contains %d/%d tuples",
            cache->cc_relname,
            cache->cc_ntup,
            u_sess->cache_cxt.cache_header->ch_ntup);
        ereport(level, (errmsg("SearchCatCache(%s): put neg entry in bucket %u", cache->cc_relname, hashIndex)));

        /*
         * We are not returning the negative entry to the caller, so leave its
         * refcount zero.
         */

        return NULL;
        }
    }

    CACHE4_elog(DEBUG2,
        "SearchCatCache(%s): Contains %d/%d tuples",
        cache->cc_relname,
        cache->cc_ntup,
        u_sess->cache_cxt.cache_header->ch_ntup);
    CACHE3_elog(DEBUG2, "SearchCatCache(%s): put in bucket %d", cache->cc_relname, hashIndex);

#ifdef CATCACHE_STATS
    cache->cc_newloads++;
#endif

    return &ct->tuple;
}



/*
 *	ReleaseCatCache
 *
 *	Decrement the reference count of a catcache entry (releasing the
 *	hold grabbed by a successful SearchCatCache).
 *
 *	NOTE: if compiled with -DCATCACHE_FORCE_RELEASE then catcache entries
 *	will be freed as soon as their refcount goes to zero.  In combination
 *	with aset.c's CLOBBER_FREED_MEMORY option, this provides a good test
 *	to catch references to already-released catcache entries.
 */
void ReleaseCatCache(HeapTuple tuple)
{
    /* dont call ReleaseCatCache , call ReleaseSysCache instead */
    Assert(!EnableLocalSysCache());
    CatCTup* ct = (CatCTup*)(((char*)tuple) - offsetof(CatCTup, tuple));

    /* Safety checks to ensure we were handed a cache entry */
    Assert(ct->ct_magic == CT_MAGIC);
    Assert(ct->refcount > 0);

    ct->refcount--;
    ResourceOwnerForgetCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);

    if (
#ifndef CATCACHE_FORCE_RELEASE
        ct->dead &&
#endif
        ct->refcount == 0 && (ct->c_list == NULL || ct->c_list->refcount == 0))
        CatCacheRemoveCTup(ct->my_cache, ct);
}

/*
 *	GetCatCacheHashValue
 *
 *		Compute the hash value for a given set of search keys.
 *
 * The reason for exposing this as part of the API is that the hash value is
 * exposed in cache invalidation operations, so there are places outside the
 * catcache code that need to be able to compute the hash values.
 */
uint32 GetCatCacheHashValue(CatCache* cache, Datum v1, Datum v2, Datum v3, Datum v4)
{
    /*
     * one-time startup overhead for each cache
     */
    if (cache->cc_tupdesc == NULL)
        CatalogCacheInitializeCache(cache);

    /*
     * calculate the hash value
     */
    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;
    return CatalogCacheComputeHashValue(cache->cc_hashfunc, cache->cc_nkeys, arguments);
}

HeapTuple CreateHeapTuple4BuiltinFuncDesc(const Builtin_func* func, TupleDesc desc)
{
    int i;
    HeapTuple tup;
    bool nulls[Natts_pg_description];
    Datum values[Natts_pg_description];

    for (i = 0; i < Natts_pg_description; i++)
    {
        nulls[i] = false;
        values[i] = (Datum)0;
    }
    values[Anum_pg_description_objoid - 1] = ObjectIdGetDatum(func->foid);
    values[Anum_pg_description_classoid - 1] = ObjectIdGetDatum(ProcedureRelationId);
    values[Anum_pg_description_objsubid - 1] = Int32GetDatum(0);
    values[Anum_pg_description_description - 1] = CStringGetTextDatum(func->descr);

    /* create tuple */
    tup = heap_form_tuple(desc, values, nulls);

    HeapTupleHeaderSetXminFrozen(tup->t_data);
    tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
    return tup;
}

/*
 * the function is used only in split the buildin functions from pg_proc systable
 * param desc: desc isn't NULL when it is called for build builtin function
 * dynamic view, because the view is used by cast and type dump,the
 * builtin function's oid and table oid is essential. Otherwise it is NULL pointer
 */
HeapTuple CreateHeapTuple4BuiltinFunc(const Builtin_func* func, TupleDesc desc)
{
    int i;
    int parameterCount;
    int allParamCount;
    bool genericInParam = false;
    bool anyrangeInParam = false;
    bool internalInParam = false;
    bool genericOutParam = false;
    bool anyrangeOutParam = false;
    bool internalOutParam = false;
    Oid relid;
    Oid variadicType = InvalidOid;
    HeapTuple tup = NULL;
    NameData procname;
    TupleDesc tupDesc;
    Datum paramNames;
    Datum parameterModes;
    Datum allParameterTypes;
    Oid* allParams = NULL;
    char* paramModes = NULL;
    oidvector* parameterTypes = NULL;
    int2vector* defargpos = NULL;
    ArrayType* arrallParameterTypes = NULL;
    Datum* allTypes = NULL;
    ArrayType* arrparameterModes = NULL;
    Datum* dtmParamModes = NULL;
    ArrayType* arrparameterNames = NULL;
    Datum* dtmParamNames = NULL;
    Acl* proacl = NULL;

    int attrCount = desc != NULL ? (Natts_pg_proc + 1) : Natts_pg_proc;
    bool nulls[attrCount];
    bool replaces[attrCount];
    Datum values[attrCount];

    /* sanity checks*/
    Assert(PointerIsValid(func->prosrc));

    parameterCount = func->proargtypes.count;
    if (parameterCount < 0 || parameterCount > FUNC_MAX_ARGS) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                errmsg_plural("functions cannot have more than %d argument",
                    "functions cannot have more than %d arguments",
                    FUNC_MAX_ARGS,
                    FUNC_MAX_ARGS)));
    }

    /* note: the above is correct, we do NOT count output arguments */
    /* Deconstruct array inputs */
    if (func->proallargtypes != NULL) {
        /* has output parameters */
        allParamCount = func->proallargtypes->count;
        if (allParamCount <= 0) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("allParameterTypes is not a 1-D Oid array")));
        }
        allParams = (Oid*)func->proallargtypes->values;
        Assert(allParamCount >= parameterCount);
    } else {
        /* has no output parameters */
        allParamCount = parameterCount;
        allParams = func->proargtypes.values;
    }

    /* Check paramode */
    if (func->proargmodes != NULL) {
        if (func->proargmodes->count == 0 || func->proargmodes->count != allParamCount ||
            func->proargmodes->values == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("parameterModes is not a 1-D char array")));
        }
        paramModes = (char*)func->proargmodes->values;
    }

    /*
     * Detect whether we have polymorphic or INTERNAL arguments.  The first
     * loop checks input arguments, the second output arguments.
     */
    for (i = 0; i < parameterCount; i++) {
        switch (func->proargtypes.values[i]) {
            case ANYARRAYOID:
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                genericInParam = true;
                break;
            case ANYRANGEOID:
                genericInParam = true;
                anyrangeInParam = true;
                break;
            case INTERNALOID:
                internalInParam = true;
                break;
            default:
                break;
        }
    }

    if (func->proallargtypes != NULL) {
        for (i = 0; i < allParamCount; i++) {
            if (paramModes == NULL || paramModes[i] == PROARGMODE_IN || paramModes[i] == PROARGMODE_VARIADIC) {
                continue;
            }

            switch (allParams[i]) {
                case ANYARRAYOID:
                case ANYELEMENTOID:
                case ANYNONARRAYOID:
                case ANYENUMOID:
                    genericOutParam = true;
                    break;
                case ANYRANGEOID:
                    genericOutParam = true;
                    anyrangeOutParam = true;
                    break;
                case INTERNALOID:
                    internalOutParam = true;
                    break;
                default:
                    break;
            }
        }
    }

    /*
     * don't allow functions of complex types that have the same name as
     * existing attributes of the type.
     * specially, if it is in bootstrapmode, this function is called by
     * bootparse.y, and system table pg_type hasn't created yet, so we should
     * ignore this check.
     */
    if (parameterCount == 1 && func->proallargtypes != NULL && OidIsValid(func->proallargtypes->values[0]) &&
        !IsBootstrapProcessingMode() && ((relid = typeidTypeRelid((func->proallargtypes->values)[0])) != InvalidOid) &&
        get_attnum(relid, func->prosrc) != InvalidAttrNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_COLUMN),
                errmsg("\"%s\" is already an attribute of type %s",
                    func->funcName,
                    format_type_be((func->proallargtypes->values)[0]))));
    }

    if (paramModes != NULL && !IsBootstrapProcessingMode()) {
        /*
         * Only the last input parameter can be variadic; if it is, save its
         * element type.  Errors here are just elog since caller should have
         * checked this already.
         */
        for (i = 0; i < allParamCount; i++) {
            switch (paramModes[i]) {
                case PROARGMODE_IN:
                case PROARGMODE_INOUT:
                    if (OidIsValid(variadicType)) {
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("variadic parameter must be last")));
                    }
                    break;
                case PROARGMODE_OUT:
                    /* okay */
                    break;
                case PROARGMODE_TABLE:
                    break;
                case PROARGMODE_VARIADIC: {
                    if (OidIsValid(variadicType)) {
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("variadic parameter must be last")));
                    }
                    switch (allParams[i]) {
                        case ANYOID:
                            variadicType = ANYOID;
                            break;
                        case ANYARRAYOID:
                            variadicType = ANYELEMENTOID;
                            break;
                        default:
                            variadicType = get_element_type(allParams[i]);
                            if (!OidIsValid(variadicType)) {
                                ereport(ERROR,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("variadic parameter is not an array")));
                            }
                            break;
                    }
                    break;
                }
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("invalid parameter mode '%c'", paramModes[i])));
                    break;
            }
        }
    }
    if (IsBootstrapProcessingMode()) {
        variadicType = func->provariadic;
    }
    /* construct the spcified struct  */
    parameterTypes = buildoidvector(func->proargtypes.values, func->proargtypes.count);
    defargpos = buildint2vector(func->prodefaultargpos->values, func->prodefaultargpos->count);

    if (func->proallargtypes != NULL) {
        allTypes = (Datum*)palloc((func->proallargtypes->count) * sizeof(Datum));
        Oid* argvalues = func->proallargtypes->values;
        for (i = 0; i < func->proallargtypes->count; i++) {
            allTypes[i] = ObjectIdGetDatum(*(argvalues + i));
        }
        arrallParameterTypes = construct_array(allTypes, func->proallargtypes->count, OIDOID, sizeof(Oid), true, 'i');
    } else {
        arrallParameterTypes = NULL;
    }

    allParameterTypes = PointerGetDatum(arrallParameterTypes);

    if (func->proargmodes != NULL) {
        dtmParamModes = (Datum*)palloc((func->proargmodes->count) * sizeof(Datum));
        char* mds = func->proargmodes->values;
        for (i = 0; i < func->proargmodes->count; i++) {
            dtmParamModes[i] = CharGetDatum(*(mds + i));
        }
        arrparameterModes = construct_array(dtmParamModes, func->proargmodes->count, CHAROID, 1, true, 'c');
    } else {
        arrparameterModes = NULL;
    }

    parameterModes = PointerGetDatum(arrparameterModes);

    if (func->proargnames != NULL) {
        dtmParamNames = (Datum*)palloc((func->proargnames->count) * sizeof(Datum));
        char** nms = func->proargnames->values;
        for (i = 0; i < func->proargnames->count; i++) {
            if (nms[i] != NULL) {
                dtmParamNames[i] = CStringGetTextDatum(nms[i]);
            } else {
                dtmParamNames[i] = CStringGetTextDatum("");
            }
        }
        arrparameterNames = construct_array(dtmParamNames, func->proargnames->count, TEXTOID, -1, false, 'i');
    } else {
        arrparameterNames = NULL;
    }
    paramNames = PointerGetDatum(arrparameterNames);

    // proargdefaults and prodefaultargpos have not handled???handle it later!!!!
    /*
     * All seems OK; prepare the data to be inserted into pg_proc.
     */
    for (i = 0; i < attrCount; ++i) {
        nulls[i] = false;
        values[i] = (Datum)0;
        replaces[i] = true;
    }

    // proconfig have not handled???handle it later!!!!
    namestrcpy(&procname, func->funcName);
    values[Anum_pg_proc_proname - 1] = NameGetDatum(&procname);
    values[Anum_pg_proc_pronamespace - 1] = ObjectIdGetDatum(func->pronamespace);
    values[Anum_pg_proc_packageid - 1] = ObjectIdGetDatum(func->propackageid);
    values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(func->proowner);
    values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(func->prolang);
    values[Anum_pg_proc_procost - 1] = Float4GetDatum(func->procost);
    values[Anum_pg_proc_prorows - 1] = Float4GetDatum(func->prorows);
    values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(variadicType);
    values[Anum_pg_proc_protransform - 1] = ObjectIdGetDatum(func->protransform);
    values[Anum_pg_proc_proisagg - 1] = BoolGetDatum(func->proisagg);
    values[Anum_pg_proc_proiswindow - 1] = BoolGetDatum(func->proiswindow);
    values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(func->prosecdef);
    values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(func->proleakproof);
    values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(func->strict);
    values[Anum_pg_proc_proretset - 1] = BoolGetDatum(func->retset);
    values[Anum_pg_proc_provolatile - 1] = CharGetDatum(func->provolatile);
    values[Anum_pg_proc_pronargs - 1] = UInt16GetDatum(parameterCount);
    values[Anum_pg_proc_pronargdefaults - 1] = UInt16GetDatum(func->pronargdefaults);
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(func->rettype);
    if (parameterCount <= FUNC_MAX_ARGS_INROW) {
        nulls[Anum_pg_proc_proargtypesext - 1] = true;
        values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes);
    } else {
        char hex[MD5_HASH_LEN + 1];
        if (!pg_md5_hash((void*)parameterTypes->values, parameterTypes->dim1 * sizeof(Oid), hex)) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }

        /* Build a dummy oidvector using the hash value and use it as proargtypes field value. */
        oidvector* dummy = buildoidvector((Oid*)hex, MD5_HASH_LEN / sizeof(Oid));
        values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(dummy);
        values[Anum_pg_proc_proargtypesext - 1] = PointerGetDatum(parameterTypes);
    }
    values[Anum_pg_proc_prokind - 1] = CharGetDatum(func->prokind);
    values[Anum_pg_proc_proisprivate - 1] = BoolGetDatum(func->proisprivate);

    if (allParameterTypes != PointerGetDatum(NULL)) {
        values[Anum_pg_proc_proallargtypes - 1] = allParameterTypes;
    } else {
        nulls[Anum_pg_proc_proallargtypes - 1] = true;
    }

    if (allParameterTypes != PointerGetDatum(NULL)) {
        /*
         * do this when the number of all paramters is too large
         */
        if (allParamCount <= FUNC_MAX_ARGS_INROW) {
            values[Anum_pg_proc_allargtypes - 1] = PointerGetDatum(allParameterTypes);
            nulls[Anum_pg_proc_allargtypesext - 1] = true;
        } else {
            /*
             * The OIDVECTOR and INT2VECTOR datatypes are storage-compatible with
             * generic arrays, but they support only one-dimensional arrays with no
             * nulls (and no null bitmap).
             */
            oidvector* dummy = MakeMd5HashOids((oidvector*)allParameterTypes);

            values[Anum_pg_proc_allargtypes - 1] = PointerGetDatum(dummy);
            values[Anum_pg_proc_allargtypesext - 1] = PointerGetDatum(allParameterTypes);
        }
    } else if (parameterTypes != PointerGetDatum(NULL)) {
        values[Anum_pg_proc_allargtypes - 1] = values[Anum_pg_proc_proargtypes - 1];
        values[Anum_pg_proc_allargtypesext - 1] = values[Anum_pg_proc_proargtypesext - 1];
        nulls[Anum_pg_proc_allargtypesext - 1] = nulls[Anum_pg_proc_proargtypesext - 1];
    } else {
        nulls[Anum_pg_proc_allargtypes - 1] = true;
        nulls[Anum_pg_proc_allargtypesext - 1] = true;
    }

    if (parameterModes != PointerGetDatum(NULL)) {
        values[Anum_pg_proc_proargmodes - 1] = parameterModes;
    } else {
        nulls[Anum_pg_proc_proargmodes - 1] = true;
    }

    if (paramNames != PointerGetDatum(NULL)) {
        values[Anum_pg_proc_proargnames - 1] = paramNames;
    } else {
        nulls[Anum_pg_proc_proargnames - 1] = true;
    }

    if (func->proargdefaults != NULL) {
        values[Anum_pg_proc_proargdefaults - 1] = CStringGetTextDatum(func->proargdefaults);
        if (parameterCount <= FUNC_MAX_ARGS_INROW) {
            values[Anum_pg_proc_prodefaultargpos - 1] = PointerGetDatum(defargpos);
            nulls[Anum_pg_proc_prodefaultargposext - 1] = true;
        } else {
            values[Anum_pg_proc_prodefaultargposext - 1] = PointerGetDatum(defargpos);
            nulls[Anum_pg_proc_prodefaultargpos - 1] = true;
        }
    } else {
        nulls[Anum_pg_proc_proargdefaults - 1] = true;
        nulls[Anum_pg_proc_prodefaultargpos - 1] = true;
        nulls[Anum_pg_proc_prodefaultargposext - 1] = true;
    }

    values[Anum_pg_proc_prosrc - 1] = CStringGetTextDatum(func->prosrc);
    if (func->fencedmode != NULL) {
        values[Anum_pg_proc_fenced - 1] = BoolGetDatum(*func->fencedmode);
    } else {
        nulls[Anum_pg_proc_fenced - 1] = true;
    }

    if (func->proshippable != NULL) {
        values[Anum_pg_proc_shippable - 1] = BoolGetDatum(*func->proshippable);
    } else {
        nulls[Anum_pg_proc_shippable - 1] = true;
    }

    if (func->propackage != NULL) {
        values[Anum_pg_proc_package - 1] = BoolGetDatum(*func->propackage);
    } else {
        nulls[Anum_pg_proc_package - 1] = true;
    }

    if (func->probin != NULL) {
        values[Anum_pg_proc_probin - 1] = CStringGetTextDatum(func->probin);
    } else {
        nulls[Anum_pg_proc_probin - 1] = true;
    }

    if (func->proconfig == NULL) {
        nulls[Anum_pg_proc_proconfig - 1] = true;
    }
    // if proconfig != NULL, it need to be handled later!
    /* First, get default permissions and set up proacl */
    proacl = get_user_default_acl(ACL_OBJECT_FUNCTION, func->proowner, func->pronamespace);
    if (proacl != NULL) {
        values[Anum_pg_proc_proacl - 1] = PointerGetDatum(proacl);
    } else {
        nulls[Anum_pg_proc_proacl - 1] = true;
    }

    if (desc != NULL) {
        values[Anum_pg_proc_oid - 1] = ObjectIdGetDatum(func->foid);
        tupDesc = desc;
    } else {
        Relation rel = heap_open(ProcedureRelationId, AccessShareLock);
        tupDesc = RelationGetDescr(rel);
        heap_close(rel, AccessShareLock);
    }

    if (func->proargsrc != NULL) {
        values[Anum_pg_proc_proargsrc - 1] = CStringGetTextDatum(func->proargsrc);
    } else {
        nulls[Anum_pg_proc_proargsrc - 1] = true;
    }

    /* create tuple */
    tup = heap_form_tuple(tupDesc, values, nulls);

    HeapTupleHeaderSetXminFrozen(tup->t_data);
    tup->t_data->t_infomask |= HEAP_XMAX_INVALID;

    if (desc == NULL) {
        // add buildin function's oid into HeapTuple if necessary
        HeapTupleSetOid(tup, func->foid);
    }

    return tup;
}

/* create a catctup, this function is only used by builtin functions */
CatCTup* CreateCatCTup(CatCache* cache, Datum* arguments, HeapTuple ntp)
{
    CatCTup* ct = NULL;
    uint32 hashValue;
    Index hashIndex;
    Dlelem* elt = NULL;

    hashValue = CatalogCacheComputeTupleHashValue(cache->id, cache->cc_keyno, cache->cc_tupdesc, cache->cc_hashfunc,
        cache->cc_reloid, cache->cc_nkeys, ntp);
    hashIndex = HASH_INDEX(hashValue, (uint32)cache->cc_nbuckets);

    for (elt = DLGetHead(&cache->cc_bucket[hashIndex]); elt; elt = DLGetSucc(elt)) {
        ct = (CatCTup*)DLE_VAL(elt);
        if (ct->dead || ct->negative) {
            continue; /* ignore dead and negative entries */
        }
        if (ct->hash_value != hashValue) {
            continue; /* quickly skip entry if wrong hash val */
        }
        /*
         * Found a match, but can't use it if it belongs to another
         * list already
         */
        if (ct->c_list) {
            continue;
        }
        break;
    }
    if (elt == NULL) {
        /* We didn't find a usable entry, so make a new one */
        ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
    }
    return ct;
}

List* SearchPgAttributeCacheList(CatCache* cache, int nkey, Datum* arguments, List* list)
{
    HeapTuple heapTuple;
    CatCTup* cTup = NULL;
    const FormData_pg_attribute* catlogAttrs = NULL;
    Dlelem* dlelem = NULL;

    Assert(nkey == 1);
    Oid relOid = ObjectIdGetDatum(arguments[0]);
    CatalogRelationBuildParam catalogDesc = GetCatalogParam(relOid);
    catlogAttrs = catalogDesc.attrs;

    if (catalogDesc.oid == InvalidOid) {
        return list;
    }
    PG_TRY();
    {
        bool hasBucketAttr = false;
        for (int16 attnum = 0; attnum < catalogDesc.natts + GetSysAttLength(hasBucketAttr); attnum++) {
            uint32 hashValue;
            Index hashIndex;
            Form_pg_attribute attr;
            FormData_pg_attribute tempAttr;
            if (attnum < catalogDesc.natts) {
                tempAttr = catlogAttrs[attnum];
                attr = &tempAttr;
            } else {
                int16 index = attnum - catalogDesc.natts;
                if (!catalogDesc.hasoids && index == 1) {
                    continue;
                }
                /* The system table does not have the bucket column or uid column, so incoming false */
                attr = SystemAttributeDefinition(-(index + 1), catalogDesc.hasoids, false, false);
                attr->attrelid = relOid;
            }
            heapTuple = GetPgAttributeAttrTuple(cache->cc_tupdesc, attr);
            cTup = NULL;
            hashValue = CatalogCacheComputeTupleHashValue(cache->id, cache->cc_keyno, cache->cc_tupdesc,
                cache->cc_hashfunc, cache->cc_reloid, cache->cc_nkeys, heapTuple);
            hashIndex = HASH_INDEX(hashValue, static_cast<uint32>(cache->cc_nbuckets));

            for (dlelem = DLGetHead(&cache->cc_bucket[hashIndex]); dlelem; dlelem = DLGetSucc(dlelem)) {
                cTup = (CatCTup *) DLE_VAL(dlelem);
                if (cTup->dead || cTup->negative)
                    continue; /* ignore dead and negative*/
                bool attnumIsNull = false;
                int curAttnum = DatumGetInt16(SysCacheGetAttr(cache->id, &cTup->tuple,
                                                                 Anum_pg_attribute_attnum, &attnumIsNull));
                /* quickly skip entry if wrong tuple*/
                if ((attnum < catalogDesc.natts && curAttnum != attnum) ||
                    (attnum >= catalogDesc.natts && curAttnum != -(attnum - catalogDesc.natts + 1))) {
                    continue;
                }

                /* Found a match, but can't use it if it belongs to another list already */
                if (cTup->c_list)
                    continue;
                break;
            }
            if (dlelem == NULL) {
                /* We didn't find a usable entry, so make a new one */
                cTup = CatalogCacheCreateEntry(cache, heapTuple, arguments, hashValue, hashIndex, false);
            }
            heap_freetuple(heapTuple);
            /*
             * Careful here: enlarge resource owner catref array, add entry to ctlist, then bump its refcount.
             * This way leaves state correct if enlarge or lappend runs out of memory_context_list
             * We use resource owner to track referenced cachetups for safety reasons. Because ctlist is now
             * built from different sources, i.e. built-in catalogs and physical relation tuples. If failure in later
             * sources is not caught, resource owner will clean up ref count for cachetups got from previous sources.
             */
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            list = lappend(list, cTup);
            cTup->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &cTup->tuple);
        }
    }
    PG_CATCH();
    {
        ReleaseTempCatList(list, cache);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return list;
}

List* SearchBuiltinProcCacheList(CatCache* cache, int nkey, Datum* arguments, List* list)
{
    int i;
    HeapTuple tup;
    CatCTup* ct = NULL;
    const FuncGroup* gfuncs = NULL;

    Assert(nkey == 1);

    char* funcname = NameStr(*(DatumGetName(arguments[0])));
    gfuncs = SearchBuiltinFuncByName(funcname);

    if (gfuncs == NULL) {
        CACHE3_elog(DEBUG2, "%s: the function \"%s\" does not in built-in list", __FUNCTION__, funcname);
        return list;
    }

    PG_TRY();
    {
        for (i = 0; i < gfuncs->fnums; i++) {
            const Builtin_func* bfunc = &gfuncs->funcs[i];
            tup = CreateHeapTuple4BuiltinFunc(bfunc, NULL);
            ct = CreateCatCTup(cache, arguments, tup);
            heap_freetuple(tup);
            /*
             * Careful here: enlarge resource owner catref array, add entry to ctlist, then bump its refcount.
             * This way leaves state correct if enlarge or lappend runs out of memory_context_list
             * We use resource owner to track referenced cachetups for safety reasons. Because ctlist is now
             * built from different sources, i.e. built-in catalogs and physical relation tuples. If failure in later
             * sources is not caught, resource owner will clean up ref count for cachetups got from previous sources.
             */
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            list = lappend(list, ct);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        }
    }
    PG_CATCH();
    {
        ReleaseTempCatList(list, cache);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return list;
}

TupleDesc CreateTupDesc4BuiltinFuncWithOid()
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(40, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "proname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "pronamespace", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "proowner", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "prolang", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "procost", FLOAT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "prorows", FLOAT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "provariadic", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8, "protransform", REGPROCOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)9, "proisagg", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)10, "proiswindow", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)11, "prosecdef", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)12, "proleakproof", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)13, "proisstrict", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)14, "proretset", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)15, "provolatile", CHAROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)16, "pronargs", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)17, "pronargdefaults", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)18, "prorettype", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)19, "proargtypes", OIDVECTOROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)20, "proallargtypes", INT4ARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)21, "proargmodes", CHARARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)22, "proargnames", TEXTARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)23, "proargdefaults", PGNODETREEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)24, "prosrc", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)25, "probin", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)26, "proconfig", TEXTARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)27, "proacl", ACLITEMARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)28, "prodefaultargpos", INT2VECTOROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)29, "fencedmode", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)30, "proshippable", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)31, "propackage", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)32, "prokind", CHAROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)33, "proargsrc", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)34, "propackageid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)35, "proisprivate", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)36, "proargtypesext", OIDVECTOREXTENDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)37, "prodefaultargposext", INT2VECTOREXTENDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)38, "allargtypes", OIDVECTOROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_proc_allargtypesext, "allargtypesext", OIDVECTOREXTENDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)40, "oid", OIDOID, -1, 0);
    return tupdesc;
}

/*
 * create a dynamic view for show the builtin functions
 * which are harding coding in funciton array
 */
Datum pv_builtin_functions(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext;
    TupleDesc tupdesc = NULL;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = CreateTupDesc4BuiltinFuncWithOid();
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    while (funcctx->call_cntr < nBuiltinFuncs) {
        HeapTuple tuple;
        const Builtin_func* func = g_sorted_funcs[funcctx->call_cntr];
        tuple = CreateHeapTuple4BuiltinFunc(func, funcctx->tuple_desc);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

void InsertBuiltinFuncInBootstrap()
{
    HeapTuple tup = NULL;
    const Builtin_func* func = NULL;

    for (int i = 0; i < g_nfuncgroups; i++) {
        const FuncGroup* fg = &g_func_groups[i];
        for (int j = 0; j < fg->fnums; j++) {
            func = &fg->funcs[j];
            tup = CreateHeapTuple4BuiltinFunc(func, NULL);
            simple_heap_insert(t_thrd.bootstrap_cxt.boot_reldesc, tup);
            heap_freetuple(tup);
        }
    }
}

void InsertBuiltinFuncDescInBootstrap()
{
    HeapTuple tup = NULL;
    const Builtin_func* func = NULL;
    Relation rel = heap_open(DescriptionRelationId, RowExclusiveLock);
    for (int i = 0; i < g_nfuncgroups; i++) {
        const FuncGroup* fg = &g_func_groups[i];
        for (int j = 0; j < fg->fnums; j++) {
            func = &fg->funcs[j];
            if (func->descr == NULL) {
                continue;
            }
            tup = CreateHeapTuple4BuiltinFuncDesc(func, RelationGetDescr(rel));
            simple_heap_insert(rel, tup);
            CatalogUpdateIndexes(rel, tup);
            heap_freetuple(tup);
        }
    }
    heap_close(rel, NoLock);
}

/*
 *	SearchCatCacheList
 *
 *		Generate a list of all tuples matching a partial key (that is,
 *		a key specifying just the first K of the cache's N key columns).
 *
 *		The caller must not modify the list object or the pointed-to tuples,
 *		and must call ReleaseCatCacheList() when done with the list.
 */
CatCList* SearchCatCacheList(CatCache* cache, int nkeys, Datum v1, Datum v2, Datum v3, Datum v4)
{
    /* dont call SearchCatCacheList , call SearchSysCacheList instead */
    Assert(!EnableLocalSysCache());

    Datum arguments[CATCACHE_MAXKEYS];
    uint32 lHashValue;
    Dlelem* elt = NULL;
    CatCList* cl = NULL;

    SearchCatCacheCheck();

    /*
     * one-time startup overhead for each cache
     */
    if (cache->cc_tupdesc == NULL)
        CatalogCacheInitializeCache(cache);

    Assert(nkeys > 0 && nkeys < cache->cc_nkeys);

#ifdef CATCACHE_STATS
    cache->cc_lsearches++;
#endif

    /* Initialize local parameter array */
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    /*
     * compute a hash value of the given keys for faster search.  We don't
     * presently divide the CatCList items into buckets, but this still lets
     * us skip non-matching items quickly most of the time.
     */
    lHashValue = CatalogCacheComputeHashValue(cache->cc_hashfunc, nkeys, arguments);

    /*
     * scan the items until we find a match or exhaust our list
     */
    for (elt = DLGetHead(&cache->cc_lists); elt; elt = DLGetSucc(elt)) {
        cl = (CatCList*)DLE_VAL(elt);

        if (cl->dead)
            continue; /* ignore dead entries */

        if (cl->hash_value != lHashValue)
            continue; /* quickly skip entry if wrong hash val */

        /*
         * see if the cached list matches our key.
         */
        if (cl->nkeys != nkeys)
            continue;

        if (!CatalogCacheCompareTuple(cache->cc_fastequal, nkeys, cl->keys, arguments))
            continue;

        /*
         * We found a matching list.  Move the list to the front of the
         * cache's list-of-lists, to speed subsequent searches.  (We do not
         * move the members to the fronts of their hashbucket lists, however,
         * since there's no point in that unless they are searched for
         * individually.)
         */
        DLMoveToFront(&cl->cache_elem);

        /* Bump the list's refcount and return it */
        ResourceOwnerEnlargeCatCacheListRefs(t_thrd.utils_cxt.CurrentResourceOwner);
        cl->refcount++;
        ResourceOwnerRememberCatCacheListRef(t_thrd.utils_cxt.CurrentResourceOwner, cl);

        CACHE2_elog(DEBUG2, "SearchCatCacheList(%s): found list", cache->cc_relname);

#ifdef CATCACHE_STATS
        cache->cc_lhits++;
#endif

        return cl;
    }

    return SearchCatCacheListMiss(cache, nkeys, arguments, lHashValue);
}

static CatCList* SearchCatCacheListMiss(CatCache* cache, int nkeys, Datum* arguments, uint32 lHashValue)
{
    Dlelem* elt = NULL;
    CatCList* cl = NULL;
    CatCTup* ct = NULL;
    List* volatile ctlist = NULL;
    ListCell* ctlist_item = NULL;
    int nmembers;
    bool ordered = false;
    HeapTuple ntp;
    MemoryContext oldcxt;
    int i;

    /*
     * List was not found in cache, so we have to build it by reading the
     * relation.  For each matching tuple found in the relation, use an
     * existing cache entry if possible, else build a new one.
     *
     * We have to bump the member refcounts temporarily to ensure they won't
     * get dropped from the cache while loading other members. We use a PG_TRY
     * block to ensure we can undo those refcounts if we get an error before
     * we finish constructing the CatCList.
     */
    ResourceOwnerEnlargeCatCacheListRefs(t_thrd.utils_cxt.CurrentResourceOwner);

    ctlist = NIL;

    /* Firstly, check the builtin functions. if there are functions
     * which has the same name with the one we want to find, lappend it
     * into the ctlist
     */
    if (IsProcCache(cache->cc_reloid) && CacheIsProcNameArgNsp(cache->id) &&
        u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ctlist = SearchBuiltinProcCacheList(cache, nkeys, arguments, ctlist);
    }

    if (IsAttributeCache(cache->cc_reloid) && IsSystemObjOid(DatumGetObjectId(arguments[0]))) {
        ctlist = SearchPgAttributeCacheList(cache, nkeys, arguments, ctlist);
    }

    PG_TRY();
    {
        ScanKeyData cur_skey[CATCACHE_MAXKEYS];
        Relation relation;
        SysScanDesc scandesc;
        errno_t rc;

        /*
         * Ok, need to make a lookup in the relation, copy the scankey and
         * fill out any per-call fields.
         */
        rc = memcpy_s(
            cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cache->cc_skey, sizeof(ScanKeyData) * cache->cc_nkeys);
        securec_check(rc, "", "");
        cur_skey[0].sk_argument = arguments[0];
        cur_skey[1].sk_argument = arguments[1];
        cur_skey[2].sk_argument = arguments[2];
        cur_skey[3].sk_argument = arguments[3];

        relation = heap_open(cache->cc_reloid, AccessShareLock);

        scandesc = systable_beginscan(
            relation, cache->cc_indexoid, IndexScanOK(cache->id), NULL, nkeys, cur_skey);

        /* The list will be ordered iff we are doing an index scan */
        ordered = (scandesc->irel != NULL);

        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
            uint32 hashValue;
            Index hashIndex;

            if (IsProcCache(cache->cc_reloid) && IsSystemObjOid(HeapTupleGetOid(ntp)) &&
                u_sess->attr.attr_common.IsInplaceUpgrade == false) {
                continue;
            }
            if (IsAttributeCache(cache->cc_reloid)) {
                bool attIsNull = false;
                Oid attrelid = DatumGetObjectId(
                    SysCacheGetAttr(cache->id, ntp, Anum_pg_attribute_attrelid, &attIsNull));
                if (IsSystemObjOid(attrelid) && IsValidCatalogParam(GetCatalogParam(attrelid))) {
                    continue;
                }
            }

            /*
             * See if there's an entry for this tuple already.
             */
            ct = NULL;
            hashValue = CatalogCacheComputeTupleHashValue(cache->id, cache->cc_keyno, cache->cc_tupdesc,
                cache->cc_hashfunc, cache->cc_reloid, cache->cc_nkeys, ntp);
            hashIndex = HASH_INDEX(hashValue, static_cast<uint32>(cache->cc_nbuckets));

            for (elt = DLGetHead(&cache->cc_bucket[hashIndex]); elt; elt = DLGetSucc(elt)) {
                ct = (CatCTup*)DLE_VAL(elt);
                /* ignore dead and negative entries */
                if (ct->dead || ct->negative)
                    continue;
                /* quickly skip entry if wrong hash val */
                if (ct->hash_value != hashValue)
                    continue;

                /* A built-in function is all in pg_proc, in upgrade senario, we skip searching
                 * the builtin functions from builtin function array. In non-upgrade mode, the function
                 * found from heap must exist in builtin array.
                 */
                if (IsProcCache(cache->cc_reloid) && IsSystemObjOid(HeapTupleGetOid(&(ct->tuple))) &&
                    u_sess->attr.attr_common.IsInplaceUpgrade == false) {
                    continue;
                }
                if (IsAttributeCache(cache->cc_reloid)) {
                    bool attIsNull = false;
                    Oid attrelid = DatumGetObjectId(SysCacheGetAttr(cache->id, &(ct->tuple),
                                   Anum_pg_attribute_attrelid, &attIsNull));
                    if (IsSystemObjOid(attrelid) && IsValidCatalogParam(GetCatalogParam(attrelid))) {
                        continue;
                    }
                }

                if (!ItemPointerEquals(&(ct->tuple.t_self), &(ntp->t_self)))
                    continue; /* not same tuple */

                /*
                 * Found a match, but can't use it if it belongs to another
                 * list already
                 */
                if (ct->c_list)
                    continue;

                break; /* A-OK */
            }

            if (elt == NULL) {
                /* We didn't find a usable entry, so make a new one */
                ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);
            }

            /*
             * Careful here: enlarge resource owner catref array, add entry to ctlist, then bump its refcount.
             * This way leaves state correct if enlarge or lappend runs out of memory_context_list
             * We use resource owner to track referenced cachetups for safety reasons. Because ctlist is now
             * built from different sources, i.e. built-in catalogs and physical relation tuples. If failure in later
             * sources is not caught, resource owner will clean up ref count for cachetups got from previous sources.
             */
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ctlist = lappend(ctlist, ct);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        }

        systable_endscan(scandesc);

        heap_close(relation, AccessShareLock);

        /*
         * Now we can build the CatCList entry.
         */
        oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        nmembers = list_length(ctlist);
        cl = (CatCList*)palloc(sizeof(CatCList) + (nmembers) * sizeof(CatCTup*));
        cl->systups = (CatCTup**)(((char *)cl) + sizeof(CatCList));

        /* Extract key values */
        CatCacheCopyKeys(cache->cc_tupdesc, nkeys, cache->cc_keyno, arguments, cl->keys);
        MemoryContextSwitchTo(oldcxt);

        /*
         * We are now past the last thing that could trigger an elog before we
         * have finished building the CatCList and remembering it in the
         * resource owner.	So it's OK to fall out of the PG_TRY, and indeed
         * we'd better do so before we start marking the members as belonging
         * to the list.
         */
    }
    PG_CATCH();
    {
        ReleaseTempCatList(ctlist, cache);
        PG_RE_THROW();
    }
    PG_END_TRY();

    cl->cl_magic = CL_MAGIC;
    cl->my_cache = cache;
    DLInitElem(&cl->cache_elem, cl);
    cl->refcount = 0; /* for the moment */
    cl->dead = false;
    cl->isnailed = false;
    cl->ordered = ordered;
    cl->nkeys = nkeys;
    cl->hash_value = lHashValue;
    cl->n_members = nmembers;

    i = 0;
    foreach (ctlist_item, ctlist) {
        cl->systups[i++] = ct = (CatCTup*)lfirst(ctlist_item);
        Assert(ct->c_list == NULL);
        ct->c_list = cl;
        /* release the temporary refcount on the member */
        Assert(ct->refcount > 0);
        ct->refcount--;
        ResourceOwnerForgetCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        /* mark list dead if any members already dead */
        if (ct->dead)
            cl->dead = true;
    }
    Assert(i == nmembers);

    DLAddHead(&cache->cc_lists, &cl->cache_elem);

    /* Finally, bump the list's refcount and return it */
    cl->refcount++;
    ResourceOwnerRememberCatCacheListRef(t_thrd.utils_cxt.CurrentResourceOwner, cl);

    CACHE3_elog(DEBUG2, "SearchCatCacheList(%s): made list of %d members", cache->cc_relname, nmembers);

    return cl;
}

void ReleaseTempCatList(const List* volatile ctlist, CatCache* cache)
{
    ListCell* ctlist_item = NULL;
    CatCTup* ct = NULL;
    foreach (ctlist_item, ctlist) {
        ct = (CatCTup*)lfirst(ctlist_item);
        Assert(ct->c_list == NULL);
        Assert(ct->refcount > 0);
        ct->refcount--;
        ResourceOwnerForgetCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);

        if (
#ifndef CATCACHE_FORCE_RELEASE
ct->dead &&
#endif
ct->refcount == 0 && (ct->c_list == NULL || ct->c_list->refcount == 0))
            CatCacheRemoveCTup(cache, ct);
    }
}

/*
 *	ReleaseCatCacheList
 *
 *	Decrement the reference count of a catcache list.
 */
void ReleaseCatCacheList(CatCList* list)
{
    /* dont call ReleaseCatCacheList , call ReleaseSysCacheList instead */
    Assert(!EnableLocalSysCache());
    /* Safety checks to ensure we were handed a cache entry */
    Assert(list->cl_magic == CL_MAGIC);
    Assert(list->refcount > 0);
    list->refcount--;
    ResourceOwnerForgetCatCacheListRef(t_thrd.utils_cxt.CurrentResourceOwner, list);

    if (
#ifndef CATCACHE_FORCE_RELEASE
        list->dead &&
#endif
        list->refcount == 0)
        CatCacheRemoveCList(list->my_cache, list);
}

/*
 * CatalogCacheCreateEntry
 *		Create a new CatCTup entry, copying the given HeapTuple and other
 *		supplied data into it.	The new entry initially has refcount 0.
 */
static CatCTup* CatalogCacheCreateEntry(
    CatCache* cache, HeapTuple ntp, Datum* arguments, uint32 hashValue, Index hashIndex, bool negative, bool isnailed)
{
    CatCTup* ct = NULL;
    HeapTuple dtp;
    MemoryContext oldcxt;

    /* negative entries have no tuple associated */
#ifdef USE_SPQ
    if (ntp || !negative) {
#else
    if (ntp) {
#endif
        int i;
        errno_t rc;
#ifdef USE_SPQ
        if (!ntp) {
            Form_pg_attribute tmp_attribute;
            Relation relation;
            int attno = DatumGetInt16(arguments[1]);
            Assert(attno > FirstLowInvalidHeapAttributeNumber && attno < 0);
            tmp_attribute = SystemAttributeDefinition(attno, false, false, false);
            relation = heap_open(cache->cc_reloid, AccessShareLock);;
            dtp = heaptuple_from_pg_attribute(relation, tmp_attribute);
            heap_close(relation, AccessShareLock);
        } else
#endif
        {
        Assert(!negative);

        /*
         * If there are any out-of-line toasted fields in the tuple, expand
         * them in-line.  This saves cycles during later use of the catcache
         * entry, and also protects us against the possibility of the toast
         * tuples being freed before we attempt to fetch them, in case of
         * something using a slightly stale catcache entry.
         */
        if (HeapTupleHasExternal(ntp))
            dtp = toast_flatten_tuple(ntp, cache->cc_tupdesc);
        else
            dtp = ntp;
        }

        /* Allocate memory for CatCTup and the cached tuple in one go */
        oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

        ct = (CatCTup*)palloc(sizeof(CatCTup) + MAXIMUM_ALIGNOF + dtp->t_len);
        ct->tuple.tupTableType = HEAP_TUPLE;
        ct->tuple.t_len = dtp->t_len;
        ct->tuple.t_self = dtp->t_self;
        ct->tuple.t_tableOid = dtp->t_tableOid;
        ct->tuple.t_bucketId = dtp->t_bucketId;
#ifdef PGXC
        ct->tuple.t_xc_node_id = dtp->t_xc_node_id;
#endif
        ct->tuple.t_xid_base = dtp->t_xid_base;
        ct->tuple.t_multi_base = dtp->t_multi_base;
        ct->tuple.t_data = (HeapTupleHeader)MAXALIGN(((char*)ct) + sizeof(CatCTup));
        /* copy tuple contents */
        rc = memcpy_s((char*)ct->tuple.t_data, dtp->t_len, (const char*)dtp->t_data, dtp->t_len);
        securec_check(rc, "", "");
        MemoryContextSwitchTo(oldcxt);

#ifdef USE_SPQ
        if ((ntp && dtp != ntp) || !ntp)
#else
        if (dtp != ntp)
#endif
            heap_freetuple_ext(dtp);

        /* extract keys - they'll point into the tuple if not by-value */
        for (i = 0; i < cache->cc_nkeys; i++) {
            Datum atp;
            bool isnull = false;

            atp = heap_getattr(&ct->tuple, cache->cc_keyno[i], cache->cc_tupdesc, &isnull);
            Assert(!isnull); 
            ct->keys[i] = atp;
        }
    } else {
        Assert(negative);
        oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        ct = (CatCTup*)palloc(sizeof(CatCTup));

        /*
         * Store keys - they'll point into separately allocated memory if not
         * by-value.
         */
        CatCacheCopyKeys(cache->cc_tupdesc, cache->cc_nkeys, cache->cc_keyno, arguments, ct->keys);
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * Finish initializing the CatCTup header, and add it to the cache's
     * linked list and counts.
     */
    ct->ct_magic = CT_MAGIC;
    ct->my_cache = cache;
    DLInitElem(&ct->cache_elem, (void*)ct);
    ct->c_list = NULL;
    ct->refcount = 0; /* for the moment */
    ct->dead = false;
    ct->isnailed = isnailed;
    ct->negative = negative;
    ct->hash_value = hashValue;

    DLAddHead(&cache->cc_bucket[hashIndex], &ct->cache_elem);

    cache->cc_ntup++;
    u_sess->cache_cxt.cache_header->ch_ntup++;

    return ct;
}

/*
 * Helper routine that frees keys stored in the keys array.
 */
void CatCacheFreeKeys(TupleDesc tupdesc, int nkeys, const int* attnos, Datum* keys)
{
    int i;

    for (i = 0; i < nkeys; i++) {
        int attnum = attnos[i];

        /* only valid system attribute is the oid, which is by value */
        if (attnum == ObjectIdAttributeNumber)
            continue;
        Assert(attnum > 0);

        if (!tupdesc->attrs[attnum - 1].attbyval) {
            void *ptr = DatumGetPointer(keys[i]);
            pfree_ext(ptr);
            keys[i] = (Datum)NULL;
        }
    }
}

/*
 * Helper routine that copies the keys in the srckeys array into the dstkeys
 * one, guaranteeing that the datums are fully allocated in the current memory
 * context.
 */
void CatCacheCopyKeys(TupleDesc tupdesc, int nkeys, const int* attnos, Datum* srckeys, Datum* dstkeys)
{
    int i;

    /*
     * XXX: memory and lookup performance could possibly be improved by
     * storing all keys in one allocation.
     */

    for (i = 0; i < nkeys; i++) {
        int attnum = attnos[i];

        if (attnum == ObjectIdAttributeNumber) {
            dstkeys[i] = srckeys[i];
        } else {
            Form_pg_attribute att = &tupdesc->attrs[(attnum - 1)];
            Datum src = srckeys[i];
            NameData srcname;

            /*
             * Must be careful in case the caller passed a C string where a
             * NAME is wanted: convert the given argument to a correctly
             * padded NAME.  Otherwise the memcpy() done by datumCopy() could
             * fall off the end of memory.
             */
            if (att->atttypid == NAMEOID) {
                namestrcpy(&srcname, DatumGetCString(src));
                src = NameGetDatum(&srcname);
            }

            dstkeys[i] = datumCopy(src, att->attbyval, att->attlen);
        }
    }
}

/*
 *	PrepareToInvalidateCacheTuple()
 *
 *	This is part of a rather subtle chain of events, so pay attention:
 *
 *	When a tuple is inserted or deleted, it cannot be flushed from the
 *	catcaches immediately, for reasons explained at the top of cache/inval.c.
 *	Instead we have to add entry(s) for the tuple to a list of pending tuple
 *	invalidations that will be done at the end of the command or transaction.
 *
 *	The lists of tuples that need to be flushed are kept by inval.c.  This
 *	routine is a helper routine for inval.c.  Given a tuple belonging to
 *	the specified relation, find all catcaches it could be in, compute the
 *	correct hash value for each such catcache, and call the specified
 *	function to record the cache id and hash value in inval.c's lists.
 *	CatalogCacheIdInvalidate will be called later, if appropriate,
 *	using the recorded information.
 *
 *	For an insert or delete, tuple is the target tuple and newtuple is NULL.
 *	For an update, we are called just once, with tuple being the old tuple
 *	version and newtuple the new version.  We should make two list entries
 *	if the tuple's hash value changed, but only one if it didn't.
 *
 *	Note that it is irrelevant whether the given tuple is actually loaded
 *	into the catcache at the moment.  Even if it's not there now, it might
 *	be by the end of the command, or there might be a matching negative entry
 *	to flush --- or other backends' caches might have such entries --- so
 *	we have to make list entries to flush it later.
 *
 *	Also note that it's not an error if there are no catcaches for the
 *	specified relation.  inval.c doesn't know exactly which rels have
 *	catcaches --- it will call this routine for any tuple that's in a
 *	system relation.
 */
void PrepareToInvalidateCacheTuple(
    Relation relation, HeapTuple tuple, HeapTuple newtuple, void (*function)(int, uint32, Oid))
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->systabcache.PrepareToInvalidateCacheTuple(
            relation, tuple, newtuple, function);
        return;
    }
    CatCache* ccp = NULL;
    Oid reloid;

    CACHE1_elog(DEBUG2, "PrepareToInvalidateCacheTuple: called");

    /*
     * sanity checks
     */
    Assert(RelationIsValid(relation));
    Assert(HeapTupleIsValid(tuple));
    Assert(PointerIsValid(function));
    Assert(u_sess->cache_cxt.cache_header != NULL);

    reloid = RelationGetRelid(relation);

    /* ----------------
     *	for each cache
     *	   if the cache contains tuples from the specified relation
     *		   compute the tuple's hash value(s) in this cache,
     *		   and call the passed function to register the information.
     * ----------------
     */

    for (ccp = u_sess->cache_cxt.cache_header->ch_caches; ccp; ccp = ccp->cc_next) {
        uint32 hashvalue;
        Oid dbid;

        if (ccp->cc_reloid != reloid)
            continue;
        if (ccp->cc_indexoid == ProcedureNameAllArgsNspIndexId && t_thrd.proc->workingVersionNum < 92470)
            continue;
        /* Just in case cache hasn't finished initialization yet... */
        if (ccp->cc_tupdesc == NULL)
            CatalogCacheInitializeCache(ccp);

        hashvalue = CatalogCacheComputeTupleHashValue(ccp->id, ccp->cc_keyno, ccp->cc_tupdesc, ccp->cc_hashfunc,
            ccp->cc_reloid, ccp->cc_nkeys, tuple);
        dbid = ccp->cc_relisshared ? (Oid)0 : u_sess->proc_cxt.MyDatabaseId;

        (*function)(ccp->id, hashvalue, dbid);

        if (newtuple) {
            uint32 newhashvalue;

            newhashvalue = CatalogCacheComputeTupleHashValue(ccp->id, ccp->cc_keyno, ccp->cc_tupdesc,
                ccp->cc_hashfunc, ccp->cc_reloid, ccp->cc_nkeys, newtuple);

            if (newhashvalue != hashvalue)
                (*function)(ccp->id, newhashvalue, dbid);
        }
    }
}

/*
 * Subroutines for warning about reference leaks.  These are exported so
 * that resowner.c can call them.
 */
void PrintCatCacheLeakWarning(HeapTuple tuple)
{
    CatCTup* ct = (CatCTup*)(((char*)tuple) - offsetof(CatCTup, tuple));

    /* Safety check to ensure we were handed a cache entry */
    Assert(ct->ct_magic == CT_MAGIC);

    ereport(WARNING,
        (errmsg("cache reference leak: cache %s (%d), tuple %u/%u has count %d",
            ct->my_cache->cc_relname,
            ct->my_cache->id,
            ItemPointerGetBlockNumber(&(tuple->t_self)),
            ItemPointerGetOffsetNumber(&(tuple->t_self)),
            ct->refcount)));
}

void PrintCatCacheListLeakWarning(CatCList* list)
{
    ereport(WARNING,
        (errmsg("cache reference leak: cache %s (%d), list has count %d",
            list->my_cache->cc_relname,
            list->my_cache->id,
            list->refcount)));
}


#ifndef ENABLE_MULTIPLE_NODES
/*
 * Specific CatalogCacheComputeTupleHashValue Function to support ProcedureCreate!
 */
static uint32 CatalogCacheComputeTupleHashValueForProcAllArgs(
    int* cc_keyno, TupleDesc cc_tupdesc, CCHashFN *cc_hashfunc, Oid cc_reloid, int nkeys, HeapTuple tuple)
{
    Assert(nkeys == PROCALLARGS_KEY_NUM);
    Assert(HeapTupleIsValid(tuple));

    Datum v1 = 0, v2 = 0, v3 = 0, v4 = 0;
    uint32 hashValue;

    bool isNull = false;

    /* Extract key fields from tuple */
    v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &isNull);
    Assert(!isNull);

    v2 = fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &isNull);
    Assert(!isNull);

    /*
     * Comput hashvaule with concat key2 and argmodes
     * Function with same paramterTypes and different argmodes
     * should have different hashkeys.
     */
    oidvector* allArgTypes = (oidvector*)DatumGetPointer(v2);
    Assert(allArgTypes != NULL);

    Datum tmp = heap_getattr(tuple, Anum_pg_proc_proargmodes, cc_tupdesc, &isNull);

    oidvector* argModes = ConvertArgModesToMd5Vector(tmp);
    oidvector* v2WithArgModes = MergeOidVector(allArgTypes, argModes);

    pfree_ext(argModes);

    Datum newKey2 = PointerGetDatum(v2WithArgModes);

    v3 = fastgetattr(tuple, cc_keyno[2], cc_tupdesc, &isNull);
    Assert(!isNull);
   
    v4 = fastgetattr(tuple, cc_keyno[3], cc_tupdesc, &isNull);
    Assert(!isNull);

    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = newKey2;
    arguments[2] = v3;
    arguments[3] = v4;
    hashValue = CatalogCacheComputeHashValue(cc_hashfunc, nkeys, arguments);

    pfree_ext(v2WithArgModes);
    return hashValue;
}

static bool SearchHashBucketWithArgModes(
    CatCache* cache, int nkeys, uint32 hashValue, const Datum* searchkeys, oidvector* argModes, HeapTuple *tuple)
{
    bool found = false;
    Dlelem* elt = NULL;
    CatCTup* ct = NULL;

    Index hashIndex = HASH_INDEX(hashValue, (uint32)cache->cc_nbuckets);
    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     */
    for (elt = DLGetHead(&cache->cc_bucket[hashIndex]); elt; elt = DLGetSucc(elt)) {

        ct = (CatCTup*)DLE_VAL(elt);

        if (ct->dead)
            continue; /* ignore dead entries */

        if (ct->hash_value != hashValue)
            continue; /* quickly skip entry if wrong hash val */

        if (!CatalogCacheCompareTuple(cache->cc_fastequal, nkeys, ct->keys, searchkeys))
            continue;

        /*
         * The comparison of hashvalue and keys is not enough.
         */
        if (!IsProArgModesEqualByTuple(&ct->tuple, cache->cc_tupdesc, argModes)) {
            continue;
        }

        /*
         * We found a match in the cache.  Move it to the front of the list
         * for its hashbucket, in order to speed subsequent searches.  (The
         * most frequently accessed elements in any hashbucket will tend to be
         * near the front of the hashbucket's list.)
         */
        DLMoveToFront(&ct->cache_elem);

        /*
         * If it's a positive entry, bump its refcount and return it. If it's
         * negative, we can report failure to the caller.
         */
        if (!ct->negative && t_thrd.utils_cxt.CurrentResourceOwner != NULL) {
            ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);

            CACHE3_elog(DEBUG2, "SearchCatCache(%s): found in bucket %d", cache->cc_relname, hashIndex);

#ifdef CATCACHE_STATS
            cache->cc_hits++;
#endif

            *tuple = &ct->tuple;
            found = true;
            break;
        }
    }
    return found;
}

/*
 * Specific SearchCatCacheMiss Function to support ProcedureCreate!
 */
static HeapTuple SearchCatCacheMissWithArgModes(CatCache* cache, int nkeys, uint32 hashValue,
    Datum* arguments, oidvector* argModes)
{
    Assert(cache->id == PROCALLARGS);

    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    Relation relation;
    SysScanDesc scandesc;
    HeapTuple ntp;
    CatCTup* ct = NULL;
    errno_t rc = EOK;

    Index hashIndex = HASH_INDEX(hashValue, (uint32)cache->cc_nbuckets);

    /*
     * Ok, need to make a lookup in the relation, copy the scankey and fill
     * out any per-call fields.
     */
    rc = memcpy_s(cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cache->cc_skey, sizeof(ScanKeyData) * nkeys);
    securec_check(rc, "", "");
    cur_skey[0].sk_argument = arguments[0];
    cur_skey[1].sk_argument = arguments[1];
    cur_skey[2].sk_argument = arguments[2];
    cur_skey[3].sk_argument = arguments[3];

    relation = heap_open(cache->cc_reloid, AccessShareLock);

    ereport(DEBUG1, (errmsg("cache->cc_reloid - %d", cache->cc_reloid)));

    scandesc = systable_beginscan(
        relation, cache->cc_indexoid, IndexScanOK(cache->id), NULL, nkeys, cur_skey);

    while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {

        /*
         * The key2 (pg_proc_allargtypes) can be duplicate in table.
         * We need to compare the proargmodes to make sure the function is correct.
         */
        if (!IsProArgModesEqualByTuple(ntp, cache->cc_tupdesc, argModes)) {
            continue;
        }

        ct = CatalogCacheCreateEntry(cache, ntp, arguments, hashValue, hashIndex, false);

        /* immediately set the refcount to 1 */
        ResourceOwnerEnlargeCatCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
        ct->refcount++;
        ResourceOwnerRememberCatCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, &ct->tuple);
        break; /* assume only one match */
    }

    systable_endscan(scandesc);

    heap_close(relation, AccessShareLock);
    /*
     * If tuple was not found, we need to build a negative cache entry
     * containing a fake tuple.  The fake tuple has the correct key columns,
     * but nulls everywhere else.
     * However, in this specific function for procallargs, we no longer build
     * negative entry any more, because when we create a overload pg-style
     * function with the same number intype parameters and different outtype
     * parameters (a in int) vs (a in int, b out int), syscache will find no
     * suitable cache tuple and make a new negative cache entry, but error will
     * raise in such case, and no one will free the negative cache entry!
     * In fact, the case metioned above should find a suitable tuple to return
     * the caller, but for the reason we adapt a suit of Specific Function
     * to support ProcedureCreate, syscache couldn't find the tuple. Someone
     * may find new methods to solve the problem and refactor this!
     */
    if (ct == NULL) {
        return NULL;
    }

    CACHE4_elog(DEBUG2,
        "SearchCatCacheMissWithArgModes(%s): Contains %d/%d tuples",
        cache->cc_relname,
        cache->cc_ntup,
        u_sess->cache_cxt.cache_header->ch_ntup);
    CACHE3_elog(DEBUG2, "SearchCatCacheMissWithArgModes(%s): put in bucket %d", cache->cc_relname, hashIndex);

#ifdef CATCACHE_STATS
    cache->cc_newloads++;
#endif

    return &ct->tuple;
}

/*
 * Specific SearchCatCache Function to support ProcedureCreate!
 */
HeapTuple SearchSysCacheForProcAllArgs(Datum v1, Datum v2, Datum v3, Datum v4, Datum proArgModes)
{

    if (EnableLocalSysCache()) {
        HeapTuple tmp = t_thrd.lsc_cxt.lsc->systabcache.SearchTupleForProcAllArgs(v1, v2, v3, v4, proArgModes);
        return tmp;
    }

    Assert(PointerIsValid(u_sess->syscache_cxt.SysCache[PROCALLARGS]));
    Assert(u_sess->syscache_cxt.SysCache[PROCALLARGS]->cc_nkeys == PROCALLARGS_KEY_NUM);

    CatCache* cache = u_sess->syscache_cxt.SysCache[PROCALLARGS];
    int nkeys = cache->cc_nkeys;
    Datum arguments[CATCACHE_MAXKEYS];
    uint32 hashValue;

    SearchCatCacheCheck();

    /*
     * one-time startup overhead for each cache
     */
    if (unlikely(cache->cc_tupdesc == NULL))
        CatalogCacheInitializeCache(cache);

#ifdef CATCACHE_STATS
    cache->cc_searches++;
#endif

    /*
     * Comput hashvaule with concat key2 and argmodes
     * Function with same paramterTypes and different argmodes
     * should have different hashkeys.
     */
    oidvector* allArgTypes = (oidvector*)DatumGetPointer(v2);

    Assert(allArgTypes != NULL);

    /* remember free the oidvectors when we no longer use them */
    oidvector* argModes = ConvertArgModesToMd5Vector(proArgModes);
    oidvector* v2WithArgModes = MergeOidVector(allArgTypes, argModes);
    Datum newKey2 = PointerGetDatum(v2WithArgModes);

    /* Initialize local parameter array */
    arguments[0] = v1;
    arguments[1] = newKey2;
    arguments[2] = v3;
    arguments[3] = v4;

    /*
     * find the hash bucket in which to look for the tuple
     */
    hashValue = CatalogCacheComputeHashValue(cache->cc_hashfunc, nkeys, arguments);

    /* reset parameter array */
    pfree_ext(v2WithArgModes);
    arguments[1] = v2;

    HeapTuple ret_tuple = NULL;
    bool found = false;
    found = SearchHashBucketWithArgModes(cache, nkeys, hashValue, arguments, argModes, &ret_tuple);
    if (found) {

        pfree_ext(argModes);
        return ret_tuple;
    }

    ret_tuple = SearchCatCacheMissWithArgModes(cache, nkeys, hashValue, arguments, argModes);

    pfree_ext(argModes);
    return ret_tuple;
}
#endif
