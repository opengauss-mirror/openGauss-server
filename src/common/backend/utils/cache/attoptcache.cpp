/* -------------------------------------------------------------------------
 *
 * attoptcache.c
 *	  Attribute options cache management.
 *
 * Attribute options are cached separately from the fixed-size portion of
 * pg_attribute entries, which are handled by the relcache.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/attoptcache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "utils/attoptcache.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/syscache.h"

/* attrelid and attnum form the lookup key, and must appear first */
typedef struct {
    Oid attrelid;
    int attnum;
} AttoptCacheKey;

typedef struct {
    AttoptCacheKey key;  /* lookup key - must be first */
    AttributeOpts* opts; /* options, or NULL if none */
} AttoptCacheEntry;

/*
 * InvalidateAttoptCacheCallback
 *		Flush all cache entries when pg_attribute is updated.
 *
 * When pg_attribute is updated, we must flush the cache entry at least
 * for that attribute.	Currently, we just flush them all.	Since attribute
 * options are not currently used in performance-critical paths (such as
 * query execution), this seems OK.
 */
static void InvalidateAttoptCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS status;
    AttoptCacheEntry* attopt = NULL;

    hash_seq_init(&status, u_sess->cache_cxt.att_opt_cache_hash);
    while ((attopt = (AttoptCacheEntry*)hash_seq_search(&status)) != NULL) {
        if (attopt->opts != NULL) {
            pfree_ext(attopt->opts);
        }
        if (hash_search(u_sess->cache_cxt.att_opt_cache_hash, (void*)&attopt->key, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("hash table corrupted")));
        }
    }
}

/*
 * InitializeAttoptCache
 *		Initialize the tablespace cache.
 */
static void InitializeAttoptCache(void)
{
    HASHCTL ctl;
    errno_t rc;

    /* Initialize the hash table. */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(AttoptCacheKey);
    ctl.entrysize = sizeof(AttoptCacheEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->cache_cxt.att_opt_cache_hash =
        hash_create("Attopt cache", 256, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    
    /* Watch for invalidation events. */
    CacheRegisterSessionSyscacheCallback(ATTNUM, InvalidateAttoptCacheCallback, (Datum)0);
}

/*
 * get_attribute_options
 *		Fetch attribute options for a specified table OID.
 */
AttributeOpts* get_attribute_options(Oid attrelid, int attnum)
{
    AttoptCacheKey key;
    AttoptCacheEntry* attopt = NULL;
    AttributeOpts* result = NULL;
    HeapTuple tp;
    errno_t rc = EOK;
    /* Find existing cache entry, if any. */
    if (!u_sess->cache_cxt.att_opt_cache_hash) {
        InitializeAttoptCache();
    }
    /* make sure any padding bits are unset */
    rc = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "\0", "\0");
    key.attrelid = attrelid;
    key.attnum = attnum;
    attopt = (AttoptCacheEntry*)hash_search(u_sess->cache_cxt.att_opt_cache_hash, (void*)&key, HASH_FIND, NULL);
    /* Not found in Attopt cache.  Construct new cache entry. */
    if (attopt == NULL) {
        AttributeOpts* opts = NULL;

        tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(attrelid), Int16GetDatum(attnum));
        /*
         * If we don't find a valid HeapTuple, it must mean someone has
         * managed to request attribute details for a non-existent attribute.
         * We treat that case as if no options were specified.
         */
        if (!HeapTupleIsValid(tp)) {
            opts = NULL;
        } else {
            Datum datum;
            bool isNull = false;

            datum = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attoptions, &isNull);
            if (isNull) {
                opts = NULL;
            } else {
                bytea* bytea_opts = attribute_reloptions(datum, false);
                if (bytea_opts == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION), errmsg("Invalid attribute relation option.")));
                }

                opts = (AttributeOpts*)MemoryContextAlloc(u_sess->cache_mem_cxt, VARSIZE(bytea_opts));

                rc = memcpy_s(opts, VARSIZE(bytea_opts), bytea_opts, VARSIZE(bytea_opts));
                securec_check(rc, "\0", "\0");
            }
            ReleaseSysCache(tp);
        }

        /*
         * It's important to create the actual cache entry only after reading
         * pg_attribute, since the read could cause a cache flush.
         */
        attopt = (AttoptCacheEntry*)hash_search(u_sess->cache_cxt.att_opt_cache_hash, (void*)&key, HASH_ENTER, NULL);
        attopt->opts = opts;
    }

    /* Return results in caller's memory context. */
    if (attopt->opts == NULL) {
        return NULL;
    }
    result = (AttributeOpts*)palloc(VARSIZE(attopt->opts));
    rc = memcpy_s(result, VARSIZE(attopt->opts), attopt->opts, VARSIZE(attopt->opts));
    securec_check(rc, "\0", "\0");
    return result;
}
