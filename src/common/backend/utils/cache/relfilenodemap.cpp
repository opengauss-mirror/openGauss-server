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
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_partition.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"

/* built first time through in InitializeRelfilenodeMap */
ScanKeyData relfilenode_skey[2];

typedef struct {
    Oid reltablespace;
    Oid relfilenode;
} RelfilenodeMapKey;

typedef struct {
    RelfilenodeMapKey key; /* lookup key - must be first */
    Oid relid;             /* pg_class.oid */
} RelfilenodeMapEntry;

/*
 * RelfilenodeMapInvalidateCallback
 *		Flush mapping entries when pg_class is updated in a relevant fashion.
 */
static void RelfilenodeMapInvalidateCallback(Datum arg, Oid relid)
{
    HASH_SEQ_STATUS status;
    RelfilenodeMapEntry* entry = NULL;
    /* callback only gets registered after creating the hash */
    Assert(u_sess->relmap_cxt.RelfilenodeMapHash != NULL);

    hash_seq_init(&status, u_sess->relmap_cxt.RelfilenodeMapHash);
    while ((entry = (RelfilenodeMapEntry*)hash_seq_search(&status)) != NULL) {
        /*
         * If relid is InvalidOid, signalling a complete reset, we must remove
         * all entries, otherwise just remove the specific relation's entry.
         * Always remove negative cache entries.
         */
        if (relid == InvalidOid ||        /* complete reset */
            entry->relid == InvalidOid || /* negative cache entry */
            entry->relid == relid) {      /* individual flushed relation */
            if (hash_search(u_sess->relmap_cxt.RelfilenodeMapHash, (void *) &entry->key, HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_RELFILENODEMAP), errmsg("hash table corrupted")));
            }
        }
    }
}

/*
 * RelfilenodeMapInvalidateCallback
 *		Initialize cache, either on first use or after a reset.
 */
static void InitializeRelfilenodeMap(void)
{
    HASHCTL ctl;
    int i;

    /* build skey */
    errno_t ret = memset_s(&relfilenode_skey, sizeof(relfilenode_skey), 0, sizeof(relfilenode_skey));
    securec_check(ret, "\0", "\0");

    for (i = 0; i < 2; i++) {
        fmgr_info_cxt(F_OIDEQ, &relfilenode_skey[i].sk_func, u_sess->cache_mem_cxt);
        relfilenode_skey[i].sk_strategy = BTEqualStrategyNumber;
        relfilenode_skey[i].sk_subtype = InvalidOid;
        relfilenode_skey[i].sk_collation = InvalidOid;
    }

    relfilenode_skey[0].sk_attno = Anum_pg_class_reltablespace;
    relfilenode_skey[1].sk_attno = Anum_pg_class_relfilenode;

    /* Initialize the hash table. */
    ret = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(ret, "\0", "\0");
    ctl.keysize = sizeof(RelfilenodeMapKey);
    ctl.entrysize = sizeof(RelfilenodeMapEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;

    /*
     * Only create the u_sess->relmap_cxt.RelfilenodeMapHash now, so we don't end up partially
     * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
     * error.
     */
    u_sess->relmap_cxt.RelfilenodeMapHash =
        hash_create("RelfilenodeMap cache", 1024, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* Watch for invalidation events. */
    CacheRegisterRelcacheCallback(RelfilenodeMapInvalidateCallback, (Datum)0);
}

/*
 * Map a relation's (tablespace, filenode) to a relation's oid and cache the
 * result.
 *
 * Returns InvalidOid if no relation matching the criteria could be found.
 */
Oid RelidByRelfilenode(Oid reltablespace, Oid relfilenode)
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
    if (u_sess->relmap_cxt.RelfilenodeMapHash == NULL)
        InitializeRelfilenodeMap();

    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace)
        reltablespace = 0;

    errno_t ret = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(ret, "\0", "\0");

    key.reltablespace = reltablespace;
    key.relfilenode = relfilenode;

    /*
     * Check cache and enter entry if nothing could be found. Even if no target
     * Check cache and return entry if one is found. Even if no target
     * relation can be found later on we store the negative match and return a
     * InvalidOid from cache. That's not really necessary for performance since
     * querying invalid values isn't supposed to be a frequent thing, but the
     * implementation is simpler this way.
     * InvalidOid from cache. That's not really necessary for performance
     * since querying invalid values isn't supposed to be a frequent thing,
     * but it's basically free.
     */
    entry = (RelfilenodeMapEntry*)hash_search(u_sess->relmap_cxt.RelfilenodeMapHash, (void*)&key, HASH_FIND, &found);

    if (found)
        return entry->relid;

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
        rc = memcpy_s(skey, sizeof(skey), relfilenode_skey, sizeof(skey));
        securec_check(rc, "", "");

        /* set scan arguments */
        skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
        skey[1].sk_argument = ObjectIdGetDatum(relfilenode);

        scandesc = systable_beginscan(relation, ClassTblspcRelfilenodeIndexId, true, SnapshotNow, 2, skey);

        found = false;

        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
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
            (RelfilenodeMapEntry*)hash_search(u_sess->relmap_cxt.RelfilenodeMapHash, (void*)&key, HASH_ENTER, &found);
        entry->relid = relid;
    }
    if (found)
        ereport(ERROR, (errcode(ERRCODE_RELFILENODEMAP), errmsg("corrupted hashtable")));

    return relid;
}
/*
 * Map a partation-relation's (tablespace, filenode) to it's parent relation's oid and
 * save partation-relation's Reltoastrelid to partationReltoastrelid.
 * Returns InvalidOid if no relation matching the criteria could be found.
 */
Oid PartitionRelidByRelfilenode(Oid reltablespace, Oid relfilenode, Oid& partationReltoastrelid)
{
    bool foundflag = false;
    SysScanDesc scandesc;
    Relation relation;
    HeapTuple ntp;
    ScanKeyData skey[2];
    Oid relid = InvalidOid;
    int rc = 0;

    /* pg_class will show 0 when the value is actually u_sess->proc_cxt.MyDatabaseTableSpace */
    if (reltablespace == u_sess->proc_cxt.MyDatabaseTableSpace)
        reltablespace = 0;

    /* For partition table in MPPDB, the relfilenode is in pg_partition instead of pg_class,
     * so here we scan the pg_partition again to get the parent's oid. */
    /* check plain relations by looking in pg_class */
    relation = heap_open(PartitionRelationId, AccessShareLock);

    rc = memcpy_s(skey, sizeof(skey), relfilenode_skey, sizeof(skey));
    securec_check(rc, "", "");
    skey[0].sk_attno = Anum_pg_partition_reltablespace;
    skey[1].sk_attno = Anum_pg_partition_relfilenode;
    skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
    skey[1].sk_argument = ObjectIdGetDatum(relfilenode);

    scandesc = systable_beginscan(relation, InvalidOid, false, SnapshotNow, 2, skey);

    while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
        if (foundflag)
            ereport(ERROR,
                (errcode(ERRCODE_RELFILENODEMAP),
                    errmsg("unexpected duplicate for tablespace %u, relfilenode %u", reltablespace, relfilenode)));
        foundflag = true;

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
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(ntp);
        partationReltoastrelid = partForm->reltoastrelid;
        relid = partForm->parentid;
    }

    systable_endscan(scandesc);
    heap_close(relation, AccessShareLock);

    return relid;
}
