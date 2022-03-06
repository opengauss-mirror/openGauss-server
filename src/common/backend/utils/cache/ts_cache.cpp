/* -------------------------------------------------------------------------
 *
 * ts_cache.c
 *	  Tsearch related object caches.
 *
 * Tsearch performance is very sensitive to performance of parsers,
 * dictionaries and mapping, so lookups should be cached as much
 * as possible.
 *
 * Once a backend has created a cache entry for a particular TS object OID,
 * the cache entry will exist for the life of the backend; hence it is
 * safe to hold onto a pointer to the cache entry while doing things that
 * might result in recognizing a cache invalidation.  Beware however that
 * subsidiary information might be deleted and reallocated somewhere else
 * if a cache inval and reval happens!	This does not look like it will be
 * a big problem as long as parser and dictionary methods do not attempt
 * any database access.
 *
 *
 * Copyright (c) 2006-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/ts_cache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "commands/defrem.h"
#include "tsearch/ts_cache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * MAXTOKENTYPE/MAXDICTSPERTT are arbitrary limits on the workspace size
 * used in lookup_ts_config_cache().  We could avoid hardwiring a limit
 * by making the workspace dynamically enlargeable, but it seems unlikely
 * to be worth the trouble.
 */
#define MAXTOKENTYPE 256
#define MAXDICTSPERTT 100

/*
 * We use this syscache callback to detect when a visible change to a TS
 * catalog entry has been made, by either our own backend or another one.
 *
 * In principle we could just flush the specific cache entry that changed,
 * but given that TS configuration changes are probably infrequent, it
 * doesn't seem worth the trouble to determine that; we just flush all the
 * entries of the related hash table.
 *
 * We can use the same function for all TS caches by passing the hash
 * table address as the "arg".
 */
static void InvalidateTSCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
    HTAB* hash = (HTAB*)DatumGetPointer(arg);
    HASH_SEQ_STATUS status;
    TSAnyCacheEntry* entry = NULL;

    hash_seq_init(&status, hash);
    while ((entry = (TSAnyCacheEntry*)hash_seq_search(&status)) != NULL)
        entry->isvalid = false;
    /* Also invalidate the current-config cache if it's pg_ts_config */
    /* when detach from thread, mark u_sess->tscache_cxt.TSCurrentConfigCache as invalid */
    if (hash == u_sess->tscache_cxt.TSConfigCacheHash)
        u_sess->tscache_cxt.TSCurrentConfigCache = InvalidOid;
}

void init_ts_parser_cache()
{
    /* First time through: initialize the hash table */
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TSParserCacheEntry);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->tscache_cxt.TSParserCacheHash =
        hash_create("Tsearch parser cache", 4, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    CacheRegisterSessionSyscacheCallback(
        TSPARSEROID, InvalidateTSCacheCallBack, PointerGetDatum(u_sess->tscache_cxt.TSParserCacheHash));
}

/*
 * Fetch parser cache entry
 */
TSParserCacheEntry* lookup_ts_parser_cache(Oid prsId)
{
    TSParserCacheEntry* entry = NULL;
    errno_t rc;

    if (u_sess->tscache_cxt.TSParserCacheHash == NULL) {
        init_ts_parser_cache();
    }

    /* Check single-entry cache */
    if (u_sess->tscache_cxt.lastUsedParser && u_sess->tscache_cxt.lastUsedParser->prsId == prsId &&
        u_sess->tscache_cxt.lastUsedParser->isvalid)
        return u_sess->tscache_cxt.lastUsedParser;

    /* Try to look up an existing entry */
    entry = (TSParserCacheEntry*)hash_search(u_sess->tscache_cxt.TSParserCacheHash, (void*)&prsId, HASH_FIND, NULL);
    if (entry == NULL || !entry->isvalid) {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        HeapTuple tp;
        Form_pg_ts_parser prs;

        tp = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
        if (!HeapTupleIsValid(tp)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for text search parser %u", prsId)));
        }
        prs = (Form_pg_ts_parser)GETSTRUCT(tp);
        /*
         * Sanity checks
         */
        if (!OidIsValid(prs->prsstart))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("text search parser %u has no prsstart method", prsId)));
        if (!OidIsValid(prs->prstoken))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("text search parser %u has no prstoken method", prsId)));
        if (!OidIsValid(prs->prsend))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("text search parser %u has no prsend method", prsId)));

        if (entry == NULL) {
            bool found = false;

            /* Now make the cache entry */
            entry = (TSParserCacheEntry*)hash_search(
                u_sess->tscache_cxt.TSParserCacheHash, (void*)&prsId, HASH_ENTER, &found);
            Assert(!found); /* it wasn't there a moment ago */
        }

        rc = memset_s(entry, sizeof(TSParserCacheEntry), 0, sizeof(TSParserCacheEntry));
        securec_check(rc, "\0", "\0");

        entry->prsId = prsId;
        entry->startOid = prs->prsstart;
        entry->tokenOid = prs->prstoken;
        entry->endOid = prs->prsend;
        entry->headlineOid = prs->prsheadline;
        entry->lextypeOid = prs->prslextype;

        ReleaseSysCache(tp);

        fmgr_info_cxt(entry->startOid, &entry->prsstart, u_sess->cache_mem_cxt);
        fmgr_info_cxt(entry->tokenOid, &entry->prstoken, u_sess->cache_mem_cxt);
        fmgr_info_cxt(entry->endOid, &entry->prsend, u_sess->cache_mem_cxt);
        if (OidIsValid(entry->headlineOid))
            fmgr_info_cxt(entry->headlineOid, &entry->prsheadline, u_sess->cache_mem_cxt);

        entry->isvalid = true;
    }

    u_sess->tscache_cxt.lastUsedParser = entry;

    return entry;
}

void init_ts_distionary_cache()
{
    /* First time through: initialize the hash table */
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TSDictionaryCacheEntry);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->tscache_cxt.TSDictionaryCacheHash =
        hash_create("Tsearch dictionary cache", 8, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    CacheRegisterSessionSyscacheCallback(
        TSDICTOID, InvalidateTSCacheCallBack, PointerGetDatum(u_sess->tscache_cxt.TSDictionaryCacheHash));
    CacheRegisterSessionSyscacheCallback(
        TSTEMPLATEOID, InvalidateTSCacheCallBack, PointerGetDatum(u_sess->tscache_cxt.TSDictionaryCacheHash));
}

/*
 * Fetch dictionary cache entry
 */
TSDictionaryCacheEntry* lookup_ts_dictionary_cache(Oid dictId)
{
    TSDictionaryCacheEntry* entry = NULL;
    errno_t rc;

    if (u_sess->tscache_cxt.TSDictionaryCacheHash == NULL) {
        init_ts_distionary_cache();
    }

    /* Check single-entry cache */
    if (u_sess->tscache_cxt.lastUsedDictionary && u_sess->tscache_cxt.lastUsedDictionary->dictId == dictId &&
        u_sess->tscache_cxt.lastUsedDictionary->isvalid)
        return u_sess->tscache_cxt.lastUsedDictionary;

    /* Try to look up an existing entry */
    entry = (TSDictionaryCacheEntry*)hash_search(
        u_sess->tscache_cxt.TSDictionaryCacheHash, (void*)&dictId, HASH_FIND, NULL);
    if (entry == NULL || !entry->isvalid) {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        HeapTuple tpdict, tptmpl;
        Form_pg_ts_dict dict;
        Form_pg_ts_template templ;
        MemoryContext saveCtx;

        tpdict = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
        if (!HeapTupleIsValid(tpdict)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for text search dictionary %u", dictId)));
        }
        dict = (Form_pg_ts_dict)GETSTRUCT(tpdict);
        /*
         * Sanity checks
         */
        if (!OidIsValid(dict->dicttemplate)) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("text search dictionary %u has no template", dictId)));
        }

        /*
         * Retrieve dictionary's template
         */
        tptmpl = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(dict->dicttemplate));
        if (!HeapTupleIsValid(tptmpl)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for text search template %u", dict->dicttemplate)));
        }
        templ = (Form_pg_ts_template)GETSTRUCT(tptmpl);
        /*
         * Sanity checks
         */
        if (!OidIsValid(templ->tmpllexize)) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                    errmsg("text search template %u has no lexize method", templ->tmpllexize)));
        }

        if (entry == NULL) {
            bool found = false;

            /* Now make the cache entry */
            entry = (TSDictionaryCacheEntry*)hash_search(
                u_sess->tscache_cxt.TSDictionaryCacheHash, (void*)&dictId, HASH_ENTER, &found);
            Assert(!found); /* it wasn't there a moment ago */

            /* Create private memory context the first time through */
            saveCtx = AllocSetContextCreate(u_sess->cache_mem_cxt,
                NameStr(dict->dictname),
                ALLOCSET_SMALL_MINSIZE,
                ALLOCSET_SMALL_INITSIZE,
                ALLOCSET_SMALL_MAXSIZE);
        } else {
            /* Clear the existing entry's private context */
            saveCtx = entry->dictCtx;
            MemoryContextResetAndDeleteChildren(saveCtx);
        }

        rc = memset_s(entry, sizeof(TSDictionaryCacheEntry), 0, sizeof(TSDictionaryCacheEntry));
        securec_check(rc, "\0", "\0");
        entry->dictId = dictId;
        entry->dictCtx = saveCtx;

        entry->lexizeOid = templ->tmpllexize;
        if (OidIsValid(templ->tmplinit)) {
            List* dictoptions = NIL;
            Datum opt;
            bool isnull = false;
            MemoryContext oldcontext;

            /*
             * Init method runs in dictionary's private memory context, and we
             * make sure the options are stored there too
             */
            oldcontext = MemoryContextSwitchTo(entry->dictCtx);

            opt = SysCacheGetAttr(TSDICTOID, tpdict, Anum_pg_ts_dict_dictinitoption, &isnull);
            if (isnull) {
                dictoptions = NIL;
            } else {
                dictoptions = deserialize_deflist(opt);
            }
            entry->dictData = DatumGetPointer(OidFunctionCall1(templ->tmplinit, PointerGetDatum(dictoptions)));

            (void)MemoryContextSwitchTo(oldcontext);
        }

        ReleaseSysCache(tptmpl);
        ReleaseSysCache(tpdict);

        fmgr_info_cxt(entry->lexizeOid, &entry->lexize, entry->dictCtx);

        entry->isvalid = true;
    }

    u_sess->tscache_cxt.lastUsedDictionary = entry;

    return entry;
}

/*
 * Initialize config cache and prepare callbacks.  This is split out of
 * lookup_ts_config_cache because we need to activate the callback before
 * caching u_sess->tscache_cxt.TSCurrentConfigCache, too.
 */
static void init_ts_config_cache(void)
{
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");

    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TSConfigCacheEntry);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->tscache_cxt.TSConfigCacheHash =
        hash_create("Tsearch configuration cache", 16, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    /* Flush cache on pg_ts_config and pg_ts_config_map changes */
    CacheRegisterSessionSyscacheCallback(
        TSCONFIGOID, InvalidateTSCacheCallBack, PointerGetDatum(u_sess->tscache_cxt.TSConfigCacheHash));
    CacheRegisterSessionSyscacheCallback(
        TSCONFIGMAP, InvalidateTSCacheCallBack, PointerGetDatum(u_sess->tscache_cxt.TSConfigCacheHash));
}

/*
 * Fetch configuration cache entry
 */
TSConfigCacheEntry* lookup_ts_config_cache(Oid cfgId)
{
    TSConfigCacheEntry* entry = NULL;

    if (u_sess->tscache_cxt.TSConfigCacheHash == NULL) {
        /* First time through: initialize the hash table */
        init_ts_config_cache();
    }

    /* Check single-entry cache */
    if (u_sess->tscache_cxt.lastUsedConfig && u_sess->tscache_cxt.lastUsedConfig->cfgId == cfgId &&
        u_sess->tscache_cxt.lastUsedConfig->isvalid) {
        return u_sess->tscache_cxt.lastUsedConfig;
    }
    /* Try to look up an existing entry */
    entry = (TSConfigCacheEntry*)hash_search(u_sess->tscache_cxt.TSConfigCacheHash, (void*)&cfgId, HASH_FIND, NULL);
    if (entry == NULL || !entry->isvalid) {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        HeapTuple tp;
        Form_pg_ts_config cfg;
        Relation maprel;
        Relation mapidx;
        ScanKeyData mapskey;
        SysScanDesc mapscan;
        HeapTuple maptup;
        ListDictionary maplists[MAXTOKENTYPE + 1];
        Oid mapdicts[MAXDICTSPERTT];
        int maxtokentype;
        int ndicts;
        Datum optdatum;
        bool isnull = false;
        int i;
        errno_t rc;

        tp = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));
        if (!HeapTupleIsValid(tp)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for text search configuration %u", cfgId)));
        }
        cfg = (Form_pg_ts_config)GETSTRUCT(tp);
        /*
         * Sanity checks
         */
        if (!OidIsValid(cfg->cfgparser)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("text search configuration %u has no parser", cfgId)));
		}
        if (entry == NULL) {
            bool found = false;

            /* Now make the cache entry */
            entry = (TSConfigCacheEntry*)hash_search(
                u_sess->tscache_cxt.TSConfigCacheHash, (void*)&cfgId, HASH_ENTER, &found);
            Assert(!found); /* it wasn't there a moment ago */
        } else {
            /* Cleanup old contents */
            if (entry->map) {
                for (i = 0; i < entry->lenmap; i++)
                    if (entry->map[i].dictIds)
                        pfree_ext(entry->map[i].dictIds);
                pfree_ext(entry->map);
            }
        }

        rc = memset_s(entry, sizeof(TSConfigCacheEntry), 0, sizeof(TSConfigCacheEntry));
        securec_check(rc, "", "");
        entry->cfgId = cfgId;
        entry->prsId = cfg->cfgparser;
        entry->opts = NULL;

        optdatum = SysCacheGetAttr(TSCONFIGOID, tp, Anum_pg_ts_config_cfoptions, &isnull);

        /*we record defined options in configuration and fill in undefined options with default value */
        bytea* bytea_opts = tsearch_config_reloptions(optdatum, false, cfg->cfgparser, true);
        if (PointerIsValid(bytea_opts)) {
            Assert(
                cfg->cfgparser == NGRAM_PARSER || cfg->cfgparser == ZHPARSER_PARSER || cfg->cfgparser == POUND_PARSER);
            entry->opts = (ParserCfOpts*)MemoryContextAlloc(u_sess->cache_mem_cxt, VARSIZE(bytea_opts));
            rc = memcpy_s(entry->opts, VARSIZE(bytea_opts), bytea_opts, VARSIZE(bytea_opts));
            securec_check(rc, "\0", "\0");
        }

        ReleaseSysCache(tp);

        /*
         * Scan pg_ts_config_map to gather dictionary list for each token type
         *
         * Because the index is on (mapcfg, maptokentype, mapseqno), we will
         * see the entries in maptokentype order, and in mapseqno order for
         * each token type, even though we didn't explicitly ask for that.
         */
        rc = memset_s(maplists, sizeof(maplists), 0, sizeof(maplists));
        securec_check(rc, "\0", "\0");

        maxtokentype = 0;
        ndicts = 0;

        ScanKeyInit(&mapskey, Anum_pg_ts_config_map_mapcfg, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(cfgId));

        maprel = heap_open(TSConfigMapRelationId, AccessShareLock);
        mapidx = index_open(TSConfigMapIndexId, AccessShareLock);
        mapscan = systable_beginscan_ordered(maprel, mapidx, SnapshotNow, 1, &mapskey);

        while ((maptup = systable_getnext_ordered(mapscan, ForwardScanDirection)) != NULL) {
            Form_pg_ts_config_map cfgmap = (Form_pg_ts_config_map)GETSTRUCT(maptup);
            int toktype = cfgmap->maptokentype;

            if (toktype <= 0 || toktype > MAXTOKENTYPE)
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("maptokentype value %d is out of range", toktype)));
            if (toktype < maxtokentype)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("too many pg_ts_config_map entries for one token type")));
            if (toktype > maxtokentype) {
                /* starting a new token type, but first save the prior data */
                if (ndicts > 0) {
                    maplists[maxtokentype].len = ndicts;
                    maplists[maxtokentype].dictIds =
                        (Oid*)MemoryContextAlloc(u_sess->cache_mem_cxt, sizeof(Oid) * ndicts);
                    rc = memcpy_s(maplists[maxtokentype].dictIds, sizeof(Oid) * ndicts, mapdicts, sizeof(Oid) * ndicts);
                    securec_check(rc, "\0", "\0");
                }
                maxtokentype = toktype;
                mapdicts[0] = cfgmap->mapdict;
                ndicts = 1;
            } else {
                /* continuing data for current token type */
                if (ndicts >= MAXDICTSPERTT)
                    ereport(ERROR,
                        (errcode(ERRCODE_TOO_MANY_ROWS),
                            errmsg("too many pg_ts_config_map entries for one token type")));
                mapdicts[ndicts++] = cfgmap->mapdict;
            }
        }

        systable_endscan_ordered(mapscan);
        index_close(mapidx, AccessShareLock);
        heap_close(maprel, AccessShareLock);

        if (ndicts > 0) {
            /* save the last token type's dictionaries */
            maplists[maxtokentype].len = ndicts;
            maplists[maxtokentype].dictIds = (Oid*)MemoryContextAlloc(u_sess->cache_mem_cxt, sizeof(Oid) * ndicts);
            rc = memcpy_s(maplists[maxtokentype].dictIds, sizeof(Oid) * ndicts, mapdicts, sizeof(Oid) * ndicts);
            securec_check(rc, "", "");

            /* and save the overall map */
            entry->lenmap = maxtokentype + 1;
            entry->map =
                (ListDictionary*)MemoryContextAlloc(u_sess->cache_mem_cxt, sizeof(ListDictionary) * entry->lenmap);
            rc = memcpy_s(
                entry->map, sizeof(ListDictionary) * entry->lenmap, maplists, sizeof(ListDictionary) * entry->lenmap);
            securec_check(rc, "", "");
        }

        entry->isvalid = true;
    }

    u_sess->tscache_cxt.lastUsedConfig = entry;

    return entry;
}

/* ---------------------------------------------------
 * GUC variable "default_text_search_config"
 * ---------------------------------------------------
 */
Oid getTSCurrentConfig(bool emitError)
{
    /* if we have a cached value, return it */
    if (OidIsValid(u_sess->tscache_cxt.TSCurrentConfigCache))
        return u_sess->tscache_cxt.TSCurrentConfigCache;

    /* fail if GUC hasn't been set up yet */
    if (u_sess->attr.attr_common.TSCurrentConfig == NULL || *u_sess->attr.attr_common.TSCurrentConfig == '\0') {
        if (emitError)
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("text search configuration isn't set")));
        else
            return InvalidOid;
    }

    if (u_sess->tscache_cxt.TSConfigCacheHash == NULL) {
        /* First time through: initialize the tsconfig inval callback */
        init_ts_config_cache();
    }

    /* Look up the config */
    u_sess->tscache_cxt.TSCurrentConfigCache =
        get_ts_config_oid(stringToQualifiedNameList(u_sess->attr.attr_common.TSCurrentConfig), !emitError);

    return u_sess->tscache_cxt.TSCurrentConfigCache;
}

/* GUC check_hook for default_text_search_config */
bool check_TSCurrentConfig(char** newval, void** extra, GucSource source)
{
    /*
     * If we aren't inside a transaction, we cannot do database access so
     * cannot verify the config name.  Must accept it on faith.
     */
    if (IsTransactionState()) {
        Oid cfg_id;
        HeapTuple tuple;
        Form_pg_ts_config cfg;
        char* buf = NULL;
        cfg_id = get_ts_config_oid(stringToQualifiedNameList(*newval), true);
        /*
         * When source == PGC_S_TEST, we are checking the argument of an ALTER
         * DATABASE SET or ALTER USER SET command.	It could be that the
         * intended use of the setting is for some other database, so we
         * should not error out if the text search configuration is not
         * present in the current database.  We issue a NOTICE instead.
         */
        if (!OidIsValid(cfg_id)) {
            if (source == PGC_S_TEST) {
                ereport(NOTICE,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("text search configuration \"%s\" does not exist", *newval)));
                return true;
            } else
                return false;
        }
        /*
         * Modify the actually stored value to be fully qualified, to ensure
         * later changes of search_path don't affect it.
         */
        tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfg_id));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for text search configuration %u", cfg_id)));
        }
        cfg = (Form_pg_ts_config)GETSTRUCT(tuple);

        buf = quote_qualified_identifier(get_namespace_name(cfg->cfgnamespace), NameStr(cfg->cfgname));

        ReleaseSysCache(tuple);

        /* GUC wants it malloc'd not palloc'd */
        pfree(*newval);
        *newval = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), buf);
        pfree(buf);
        buf = NULL;
        if (!*newval) {
            return false;
        }
    }

    return true;
}

/* GUC assign_hook for default_text_search_config */
void assign_TSCurrentConfig(const char* newval, void* extra)
{
    /* Just reset the cache to force a lookup on first use */
    u_sess->tscache_cxt.TSCurrentConfigCache = InvalidOid;
}
