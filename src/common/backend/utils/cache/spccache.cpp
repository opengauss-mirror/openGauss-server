/* -------------------------------------------------------------------------
 *
 * spccache.c
 *	  Tablespace cache management.
 *
 * We cache the parsed version of spcoptions for each tablespace to avoid
 * needing to reparse on every lookup.	Right now, there doesn't appear to
 * be a measurable performance gain from doing this, but that might change
 * in the future as we add more options.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/spccache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/spccache.h"
#include "utils/syscache.h"

typedef struct {
    Oid oid;              /* lookup key - must be first */
    TableSpaceOpts* opts; /* options, or NULL if none */
} TableSpaceCacheEntry;

/*
 * InvalidateTableSpaceCacheCallback
 *		Flush all cache entries when pg_tablespace is updated.
 *
 * When pg_tablespace is updated, we must flush the cache entry at least
 * for that tablespace.  Currently, we just flush them all.  This is quick
 * and easy and doesn't cost much, since there shouldn't be terribly many
 * tablespaces, nor do we expect them to be frequently modified.
 */
void InvalidateTableSpaceCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS status;
    TableSpaceCacheEntry* spc = NULL;
    struct HTAB *TableSpaceCacheHash = GetTableSpaceCacheHash();
    hash_seq_init(&status, TableSpaceCacheHash);
    while ((spc = (TableSpaceCacheEntry*)hash_seq_search(&status)) != NULL) {
        if (spc->opts != NULL)
            pfree_ext(spc->opts);
        if (hash_search(TableSpaceCacheHash, (void*)&spc->oid, HASH_REMOVE, NULL) == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("hash table corrupted")));
    }
}

/*
 * InitializeTableSpaceCache
 *		Initialize the tablespace cache.
 */
static void InitializeTableSpaceCache(void)
{
    HASHCTL ctl;
    errno_t rc;

    /* Initialize the hash table. */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TableSpaceCacheEntry);
    ctl.hash = oid_hash;
    if (EnableLocalSysCache()) {
        ctl.hcxt = t_thrd.lsc_cxt.lsc->lsc_share_memcxt;
        t_thrd.lsc_cxt.lsc->TableSpaceCacheHash =
            hash_create("TableSpace cache", 16, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
        /* Watch for invalidation events. */
        CacheRegisterThreadSyscacheCallback(TABLESPACEOID, InvalidateTableSpaceCacheCallback, (Datum)0);
    } else {
        ctl.hcxt = u_sess->cache_mem_cxt;
        u_sess->cache_cxt.TableSpaceCacheHash =
        hash_create("TableSpace cache", 16, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
        /* Watch for invalidation events. */
        CacheRegisterSessionSyscacheCallback(TABLESPACEOID, InvalidateTableSpaceCacheCallback, (Datum)0);
    }
    
}

/*
 * get_tablespace
 *		Fetch TableSpaceCacheEntry structure for a specified table OID.
 *
 * Pointers returned by this function should not be stored, since a cache
 * flush will invalidate them.
 */
static TableSpaceCacheEntry* get_tablespace(Oid spcid)
{
    TableSpaceCacheEntry* spc = NULL;
    HeapTuple tp;
    TableSpaceOpts* opts = NULL;

    /*
     * Since spcid is always from a pg_class tuple, InvalidOid implies the
     * default.
     */
    spcid = ConvertToRelfilenodeTblspcOid(spcid);

    /* Find existing cache entry, if any. */
    if (!GetTableSpaceCacheHash())
        InitializeTableSpaceCache();
    spc = (TableSpaceCacheEntry*)hash_search(GetTableSpaceCacheHash(), (void*)&spcid, HASH_FIND, NULL);
    if (spc != NULL)
        return spc;

    /*
     * Not found in TableSpace cache.  Check catcache.	If we don't find a
     * valid HeapTuple, it must mean someone has managed to request tablespace
     * details for a non-existent tablespace.  We'll just treat that case as
     * if no options were specified.
     */
    tp = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcid));
    if (!HeapTupleIsValid(tp))
        opts = NULL;
    else {
        Datum datum;
        bool isNull = false;
        errno_t rc;

        datum = SysCacheGetAttr(TABLESPACEOID, tp, Anum_pg_tablespace_spcoptions, &isNull);
        if (isNull)
            opts = NULL;
        else {
            bytea* bytea_opts = tablespace_reloptions(datum, false);
            if (bytea_opts == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION), errmsg("Invalid tablespace relation option.")));
            }

            opts = (TableSpaceOpts*)MemoryContextAlloc(LocalSharedCacheMemCxt(), VARSIZE(bytea_opts));
            rc = memcpy_s(opts, VARSIZE(bytea_opts), bytea_opts, VARSIZE(bytea_opts));
            securec_check(rc, "", "");
        }
        ReleaseSysCache(tp);
    }

    /*
     * Now create the cache entry.	It's important to do this only after
     * reading the pg_tablespace entry, since doing so could cause a cache
     * flush.
     */
    spc = (TableSpaceCacheEntry*)hash_search(GetTableSpaceCacheHash(), (void*)&spcid, HASH_ENTER, NULL);
    spc->opts = opts;
    return spc;
}

/*
 * get_tablespace_page_costs
 *		Return random and/or sequential page costs for a given tablespace.
 */
void get_tablespace_page_costs(Oid spcid, double* spc_random_page_cost, double* spc_seq_page_cost)
{
    TableSpaceCacheEntry* spc = get_tablespace(spcid);

    Assert(spc != NULL);

    if (spc_random_page_cost != NULL) {
        if (spc->opts == NULL || spc->opts->random_page_cost < 0)
            *spc_random_page_cost = u_sess->attr.attr_sql.random_page_cost;
        else
            *spc_random_page_cost = spc->opts->random_page_cost;
    }

    if (spc_seq_page_cost != NULL) {
        if (spc->opts == NULL || spc->opts->seq_page_cost < 0)
            *spc_seq_page_cost = u_sess->attr.attr_sql.seq_page_cost;
        else
            *spc_seq_page_cost = spc->opts->seq_page_cost;
    }
}
