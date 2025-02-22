/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 *
 * relfilenodemap.cpp
 *	  relfilenode to oid mapping cache.
 *
 * IDENTIFICATION
 *	  src/common/backend/utils/cache/relfilenodemap.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
// include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_partition.h"
#include "miscadmin.h"
#include "storage/smgr/segment.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/knl_localsysdbcache.h"

static const int HASH_ELEM_SIZE = 1024;
static const int PARTITION_HASH_ELEM_SIZE = 4096;
static const int filenodeSkeyNum = 2;

typedef struct {
    Oid reltablespace;
    Oid relfilenode;
} RelfilenodeMapKey;

typedef struct {
    RelfilenodeMapKey key; /* lookup key - must be first */
    Oid relid;             /* pg_class.oid */
} RelfilenodeMapEntry;

typedef struct {
    Oid reltablespace;
    Oid relfilenode;
} PartfilenodeMapKey;

typedef struct {
    PartfilenodeMapKey key; /* lookup key - must be first */
    Oid relid;             /* pg_partition.oid */
    Oid reltoastrelid;     /* pg_partition.reltoastrelid */
} PartfilenodeMapEntry;

/*
 * RelfilenodeMapInvalidateCallback
 *		Flush mapping entries when pg_class is updated in a relevant fashion.
 */
void RelfilenodeMapInvalidateCallback(Datum arg, Oid relid)
{
    HASH_SEQ_STATUS status;
    RelfilenodeMapEntry* entry = NULL;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* callback only gets registered after creating the hash */
    Assert(relmap_cxt->RelfilenodeMapHash != NULL);
    if (relmap_cxt->RelfilenodeMapHash == NULL) {
        return;
    }

    hash_seq_init(&status, relmap_cxt->RelfilenodeMapHash);
    while ((entry = (RelfilenodeMapEntry*)hash_seq_search(&status)) != NULL) {
        /*
         * If relid is InvalidOid, signalling a complete reset, we must remove
         * all entries, otherwise just remove the specific relation's entry.
         * Always remove negative cache entries.
         */
        if (relid == InvalidOid ||        /* complete reset */
            entry->relid == InvalidOid || /* negative cache entry */
            entry->relid == relid) {      /* individual flushed relation */
            if (hash_search(relmap_cxt->RelfilenodeMapHash, (void *) &entry->key, HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_RELFILENODEMAP), errmsg("hash table corrupted")));
            }
        }
    }
}

void PartfilenodeMapInvalidateCallback(Datum arg, Oid relid)
{
    HASH_SEQ_STATUS status;
    PartfilenodeMapEntry* entry = NULL;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* callback only gets registered after creating the hash */
    Assert(relmap_cxt->PartfilenodeMapHash != NULL);

    hash_seq_init(&status, relmap_cxt->PartfilenodeMapHash);
    while ((entry = (PartfilenodeMapEntry*)hash_seq_search(&status)) != NULL) {
        /*
         * If relid is InvalidOid, signalling a complete reset, we must remove
         * all entries, otherwise just remove the specific relation's entry.
         * Always remove negative cache entries.
         */
        if (relid == InvalidOid ||        /* complete reset */
            entry->relid == InvalidOid || /* negative cache entry */
            entry->relid == relid) {      /* individual flushed relation */
            if (hash_search(relmap_cxt->PartfilenodeMapHash,
                (void *) &entry->key, HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_RELFILENODEMAP), errmsg("hash table corrupted")));
            }
        }
    }
}

/*
 * RelfilenodeMapInvalidateCallback
 *		Initialize cache, either on first use or after a reset.
 */
static void InitializeRelfilenodeMap()
{
    int i;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* build skey */
    errno_t ret = memset_s(&relmap_cxt->relfilenodeSkey, sizeof(relmap_cxt->relfilenodeSkey), 0,
        sizeof(relmap_cxt->relfilenodeSkey));
    securec_check(ret, "\0", "\0");

    for (i = 0; i < filenodeSkeyNum; i++) {
        fmgr_info_cxt(F_OIDEQ, &relmap_cxt->relfilenodeSkey[i].sk_func, LocalMyDBCacheMemCxt());
        relmap_cxt->relfilenodeSkey[i].sk_strategy = BTEqualStrategyNumber;
        relmap_cxt->relfilenodeSkey[i].sk_subtype = InvalidOid;
        relmap_cxt->relfilenodeSkey[i].sk_collation = InvalidOid;
    }

    relmap_cxt->relfilenodeSkey[0].sk_attno = Anum_pg_class_reltablespace;
    relmap_cxt->relfilenodeSkey[1].sk_attno = Anum_pg_class_relfilenode;

    /* Initialize the hash table. */
    HASHCTL ctl;
    ret = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(ret, "\0", "\0");
    ctl.keysize = sizeof(RelfilenodeMapKey);
    ctl.entrysize = sizeof(RelfilenodeMapEntry);
    ctl.hash = tag_hash;
    if (EnableLocalSysCache()) {
        Assert(t_thrd.lsc_cxt.lsc->relmap_cxt.RelfilenodeMapHash == NULL);
        ctl.hcxt = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;

        /*
         * Only create the relmap_cxt->RelfilenodeMapHash now, so we don't end up partially
         * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
         * error.
         */
        t_thrd.lsc_cxt.lsc->relmap_cxt.RelfilenodeMapHash =
            hash_create("RelfilenodeMap cache", 1024, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /* Watch for invalidation events. */
        CacheRegisterThreadRelcacheCallback(RelfilenodeMapInvalidateCallback, (Datum)0);
    } else {
        ctl.hcxt = u_sess->cache_mem_cxt;
        /*
         * Only create the relmap_cxt->RelfilenodeMapHash now, so we don't end up partially
         * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
         * error.
         */
        u_sess->relmap_cxt.RelfilenodeMapHash =
            hash_create("RelfilenodeMap cache", 1024, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /* Watch for invalidation events. */
        CacheRegisterSessionRelcacheCallback(RelfilenodeMapInvalidateCallback, (Datum)0);
    }
}

static void InitializePartfilenodeMap()
{
    int i;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* build skey */
    errno_t ret = memset_s(&relmap_cxt->partfilenodeSkey, sizeof(relmap_cxt->partfilenodeSkey), 0,
        sizeof(relmap_cxt->partfilenodeSkey));
    securec_check(ret, "\0", "\0");

    for (i = 0; i < filenodeSkeyNum; i++) {
        fmgr_info_cxt(F_OIDEQ, &relmap_cxt->partfilenodeSkey[i].sk_func, LocalMyDBCacheMemCxt());
        relmap_cxt->partfilenodeSkey[i].sk_strategy = BTEqualStrategyNumber;
        relmap_cxt->partfilenodeSkey[i].sk_subtype = InvalidOid;
        relmap_cxt->partfilenodeSkey[i].sk_collation = InvalidOid;
    }

    relmap_cxt->partfilenodeSkey[0].sk_attno = Anum_pg_partition_reltablespace;
    relmap_cxt->partfilenodeSkey[1].sk_attno = Anum_pg_partition_relfilenode;

    /* Initialize the hash table. */
    HASHCTL ctl;
    ret = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(ret, "\0", "\0");
    ctl.keysize = sizeof(PartfilenodeMapKey);
    ctl.entrysize = sizeof(PartfilenodeMapEntry);
    ctl.hash = tag_hash;
    if (EnableLocalSysCache()) {
        Assert(t_thrd.lsc_cxt.lsc->relmap_cxt.PartfilenodeMapHash == NULL);
        ctl.hcxt = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;

        /*
         * Only create the relmap_cxt->PartfilenodeMapHash now, so we don't end up partially
         * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
         * error.
         */
        t_thrd.lsc_cxt.lsc->relmap_cxt.PartfilenodeMapHash = hash_create("PartfilenodeMap cache",
            PARTITION_HASH_ELEM_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /* Watch for invalidation events. */
        CacheRegisterThreadRelcacheCallback(PartfilenodeMapInvalidateCallback, (Datum)0);
    } else {
        ctl.hcxt = u_sess->cache_mem_cxt;
        /*
         * Only create the relmap_cxt->PartfilenodeMapHash now, so we don't end up partially
         * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
         * error.
         */
        u_sess->relmap_cxt.PartfilenodeMapHash = hash_create("PartfilenodeMap cache",
            PARTITION_HASH_ELEM_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /* Watch for invalidation events. */
        CacheRegisterSessionRelcacheCallback(PartfilenodeMapInvalidateCallback, (Datum)0);
    }
}

Oid RelidByRelfilenodeCache(Oid reltablespace, Oid relfilenode)
{
    RelfilenodeMapKey key;
    bool found = true;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->RelfilenodeMapHash == NULL) {
        InitializeRelfilenodeMap();
    }
    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace) {
        reltablespace = 0;
    }

    errno_t ret = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(ret, "\0", "\0");

    key.reltablespace = reltablespace;
    key.relfilenode = relfilenode;

    RelfilenodeMapEntry* entry =
        (RelfilenodeMapEntry*)hash_search(relmap_cxt->RelfilenodeMapHash, (void*)&key, HASH_FIND, &found);

    if (found) {
        return entry->relid;
    }
    return InvalidOid;
}

/*
 * Map a relation's (tablespace, filenode) to a relation's oid and cache the
 * result.
 *
 * Returns InvalidOid if no relation matching the criteria could be found.
 */
Oid RelidByRelfilenode(Oid reltablespace, Oid relfilenode, bool segment)
{
    RelfilenodeMapKey key;
    RelfilenodeMapEntry* entry = NULL;
    bool found = false;
    SysScanDesc scandesc;
    Relation relation;
    HeapTuple ntp;
    ScanKeyData skey[2];
    Oid relid;
    int rc = 0;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->RelfilenodeMapHash == NULL) {
        InitializeRelfilenodeMap();
    }

    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace)
        reltablespace = 0;

    errno_t ret = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(ret, "\0", "\0");

    key.reltablespace = reltablespace;
    key.relfilenode = relfilenode;

    /* ok, no previous cache entry, do it the hard way */

    /* initialize empty/negative cache entry before doing the actual lookups */
    relid = InvalidOid;

    /* check shared tables */
    if (reltablespace == GLOBALTABLESPACE_OID) {
        /*
         * Ok, shared table, check relmapper.
         */
        relid = RelationMapFilenodeToOid(relfilenode, true);
    } else {
        /*
         * Not a shared table, could either be a plain relation or a
         * non-shared, nailed one, like e.g. pg_class.
         */

        /* check plain relations by looking in pg_class */
        relation = heap_open(RelationRelationId, AccessShareLock);

        /* copy scankey to local copy, it will be modified during the scan */
        rc = memcpy_s(skey, sizeof(skey), relmap_cxt->relfilenodeSkey, sizeof(skey));
        securec_check(rc, "", "");

        /* set scan arguments */
        skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
        skey[1].sk_argument = ObjectIdGetDatum(relfilenode);

        /*
         * Using historical snapshot in logic decoding.
         */
        scandesc = systable_beginscan(relation, ClassTblspcRelfilenodeIndexId, true, NULL, 2, skey);

        found = false;

        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
            bool isNull = false;
            Datum datum = heap_getattr(ntp, Anum_pg_class_relbucket, RelationGetDescr(relation), &isNull);
            (void) datum;

            if ((isNull && segment) || (!isNull && !segment)) {
                /* segment table's relbucket is not null; non-segment table's relbucket must be null */
                continue;
            }

            if (found)
                ereport(ERROR,
                    (errcode(ERRCODE_RELFILENODEMAP),
                        errmsg("unexpected duplicate for tablespace %u, relfilenode %u", reltablespace, relfilenode)));

            found = true;

#ifdef USE_ASSERT_CHECKING
            if (assert_enabled) {
                Oid check;
                bool isnull = false;
                check = fastgetattr(ntp, Anum_pg_class_reltablespace, RelationGetDescr(relation), &isnull);
                Assert(!isnull && check == reltablespace);

                check = fastgetattr(ntp, Anum_pg_class_relfilenode, RelationGetDescr(relation), &isnull);
                Assert(!isnull && check == relfilenode);
            }
#endif
            relid = HeapTupleGetOid(ntp);
        }

        systable_endscan(scandesc);
        heap_close(relation, AccessShareLock);
        /* check for tables that are mapped but not shared */
        if (!found)
            relid = RelationMapFilenodeToOid(relfilenode, false);
    }

    /*
     * Only enter entry into cache now, our opening of pg_class could have
     * caused cache invalidations to be executed which would have deleted a
     * new entry if we had entered it above.
     */
    if (relid != InvalidOid) {
        entry =
            (RelfilenodeMapEntry*)hash_search(relmap_cxt->RelfilenodeMapHash, (void*)&key, HASH_ENTER, &found);
        entry->relid = relid;
    }
    if (found)
        ereport(ERROR, (errcode(ERRCODE_RELFILENODEMAP), errmsg("corrupted hashtable")));

    return relid;
}

Oid PartitionRelidByRelfilenodeCache(Oid reltablespace, Oid relfilenode, Oid &partationReltoastrelid)
{
    PartfilenodeMapKey key;
    bool found = false;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->PartfilenodeMapHash == NULL) {
        InitializePartfilenodeMap();
    }

    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace) {
        reltablespace = 0;
    }

    key.reltablespace = reltablespace;
    key.relfilenode = relfilenode;
    /*
     * Check cache and return entry if one is found. Even if no target
     * relation can be found later on we store the negative match and return a
     * InvalidOid from cache. That's not really necessary for performance since
     * querying invalid values isn't supposed to be a frequent thing, but the
     * implementation is simpler this way.
     */
    PartfilenodeMapEntry *entry =
        (PartfilenodeMapEntry*)hash_search(relmap_cxt->PartfilenodeMapHash, (void*)&key, HASH_FIND, &found);
 
    if (found) {
        partationReltoastrelid = entry->reltoastrelid;
        return entry->relid;
    }
    return InvalidOid;
}

/*
 * Map a partation-relation's (tablespace, filenode) to it's parent relation's oid and
 * save partation-relation's Reltoastrelid to partationReltoastrelid.
 * Returns InvalidOid if no relation matching the criteria could be found.
 */
Oid PartitionRelidByRelfilenode(Oid reltablespace, Oid relfilenode, Oid &partationReltoastrelid, Oid *partitionOid,
                                bool segment)
{
    PartfilenodeMapKey key;
    PartfilenodeMapEntry* entry = NULL;
    bool found = false;
    SysScanDesc scandesc;
    Relation relation;
    HeapTuple ntp;
    ScanKeyData skey[2];
    Oid relid = InvalidOid;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->PartfilenodeMapHash == NULL)
        InitializePartfilenodeMap();

    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace)
        reltablespace = 0;

    key.reltablespace = reltablespace;
    key.relfilenode = relfilenode;

    /* ok, no previous cache entry, do it the hard way */

    /* initialize empty/negative cache entry before doing the actual lookups */
    relid = InvalidOid;

    /* For partition table in MPPDB, the relfilenode is in pg_partition instead of pg_class,
     * so here we scan the pg_partition again to get the parent's oid. */
    /* check plain relations by looking in pg_class */
    relation = heap_open(PartitionRelationId, AccessShareLock);

    errno_t rc = memcpy_s(skey, sizeof(skey), GetRelMapCxt()->partfilenodeSkey, sizeof(skey));
    securec_check(rc, "", "");

    /* set scan arguments */
    int skeyNum = 2;
    skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
    skey[1].sk_argument = ObjectIdGetDatum(relfilenode);

    if (IndexGetRelation(PartitionTblspcRelfilenodeIndexId, true) == PartitionRelationId) {
        /* decode WAL produced after creating pg_partition_tblspc_relfilenode_index upgrade */
        scandesc = systable_beginscan(relation, PartitionTblspcRelfilenodeIndexId, true, NULL, skeyNum, skey);
    } else {
        /* decode WAL produced before creating pg_partition_tblspc_relfilenode_index upgrade */
        scandesc = systable_beginscan(relation, InvalidOid, false, NULL, skeyNum, skey);
    }

    while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(ntp);
        partationReltoastrelid = partForm->reltoastrelid;
        relid = partForm->parentid;
        if (partForm->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
            HeapTuple partitionTupleRaw = NULL;
            Form_pg_partition partitionTuple = NULL;
            partitionTupleRaw = SearchSysCache1((int)PARTRELID, ObjectIdGetDatum(relid));
            if (!PointerIsValid(partitionTupleRaw)) {
                relid = InvalidOid;
                ReleaseSysCache(partitionTupleRaw);
                break;
            }
            partitionTuple = (Form_pg_partition)GETSTRUCT(partitionTupleRaw);
            relid = partitionTuple->parentid;
            ReleaseSysCache(partitionTupleRaw);
        }

        HeapTuple pg_class_tuple = ScanPgRelation(relid, true, false);
        if (!HeapTupleIsValid(pg_class_tuple)) {
            /* parent is removed from pg_class. */
            relid = InvalidOid;
            break;
        }
        bool isNull;
        heap_getattr(pg_class_tuple, Anum_pg_class_relbucket, GetDefaultPgClassDesc(), &isNull);
        StorageType st = isNull ? HEAP_DISK : SEGMENT_PAGE;
        heap_freetuple_ext(pg_class_tuple);

        if ((st == SEGMENT_PAGE && !segment) || (st == HEAP_DISK && segment)) {
            relid = InvalidOid;
            continue;
        }
        if (found)
            ereport(ERROR,
                (errcode(ERRCODE_RELFILENODEMAP),
                    errmsg("unexpected duplicate for tablespace %u, relfilenode %u", reltablespace, relfilenode)));
        found = true;

#ifdef USE_ASSERT_CHECKING
        if (assert_enabled) {
            bool isnull = true;
            Oid check;
            check = fastgetattr(ntp, Anum_pg_partition_reltablespace, RelationGetDescr(relation), &isnull);
            Assert(!isnull && check == reltablespace);

            check = fastgetattr(ntp, Anum_pg_partition_relfilenode, RelationGetDescr(relation), &isnull);
            Assert(!isnull && check == relfilenode);
        }
#endif
        if (partitionOid != NULL) {
            *partitionOid = HeapTupleGetOid(ntp);
        }
    }

    systable_endscan(scandesc);
    heap_close(relation, AccessShareLock);

    /*
     * Only enter entry into cache now, our opening of pg_partition could have
     * caused cache invalidations to be executed which would have deleted a
     * new entry if we had entered it above.
     */
    if (relid != InvalidOid) {
        entry =
            (PartfilenodeMapEntry*)hash_search(relmap_cxt->PartfilenodeMapHash, (void*)&key, HASH_ENTER, &found);
        entry->relid = relid;
        entry->reltoastrelid = partationReltoastrelid;
    }

    return relid;
}

Oid HeapGetRelid(Oid reltablespace, Oid relfilenode, Oid &partationReltoastrelid, Oid *partitionOid, bool segment)
{
    Oid reloid = RelidByRelfilenodeCache(reltablespace, relfilenode);
    if (reloid == InvalidOid) {
        reloid = PartitionRelidByRelfilenodeCache(reltablespace, relfilenode, partationReltoastrelid);
    }

    if (reloid == InvalidOid) {
        reloid = RelidByRelfilenode(reltablespace, relfilenode, segment);
    }

    if (reloid == InvalidOid) {
        reloid = PartitionRelidByRelfilenode(reltablespace, relfilenode, partationReltoastrelid, partitionOid, segment);
    }
    return reloid;
}

