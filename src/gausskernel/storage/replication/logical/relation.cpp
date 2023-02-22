/* -------------------------------------------------------------------------
 * relation.c
 * 	   PostgreSQL logical replication relation mapping cache
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 	  src/backend/replication/logical/relation.c
 *
 * NOTES
 * 	  Routines in this file mainly have to do with mapping the properties
 *	  of local replication target relations to the properties of their
 *	  remote counterpart.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "access/heapam.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/inval.h"
#include "catalog/pg_subscription_rel.h"

static const int DEFAULT_LOGICAL_RELMAP_HASH_ELEM = 128;
/*
 * Relcache invalidation callback for our relation map cache.
 */
static void logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid)
{
    LogicalRepRelMapEntry *entry;

    /* Just to be sure. */
    if (t_thrd.applyworker_cxt.logicalRepRelMap == NULL)
        return;

    if (reloid != InvalidOid) {
        HASH_SEQ_STATUS status;

        hash_seq_init(&status, t_thrd.applyworker_cxt.logicalRepRelMap);

        /* use inverse lookup hashtable? */
        while ((entry = (LogicalRepRelMapEntry *)hash_seq_search(&status)) != NULL) {
            if (entry->localreloid == reloid) {
                entry->localrelvalid = false;
                hash_seq_term(&status);
                break;
            }
        }
    } else {
        /* invalidate all cache entries */
        HASH_SEQ_STATUS status;

        hash_seq_init(&status, t_thrd.applyworker_cxt.logicalRepRelMap);

        while ((entry = (LogicalRepRelMapEntry *)hash_seq_search(&status)) != NULL)
            entry->localrelvalid = false;
    }
}

/*
 * Initialize the relation map cache.
 */
static void logicalrep_relmap_init()
{
    HASHCTL ctl;

    if (!t_thrd.applyworker_cxt.logicalRepRelMapContext) {
        t_thrd.applyworker_cxt.logicalRepRelMapContext =
            AllocSetContextCreate(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT),
                                  "LogicalRepRelMapContext", ALLOCSET_DEFAULT_SIZES);
    }

    /* Initialize the relation hash table. */
    int rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(LogicalRepRelId);
    ctl.entrysize = sizeof(LogicalRepRelMapEntry);
    ctl.hcxt = t_thrd.applyworker_cxt.logicalRepRelMapContext;

    t_thrd.applyworker_cxt.logicalRepRelMap = hash_create("logicalrep relation map cache",
        DEFAULT_LOGICAL_RELMAP_HASH_ELEM, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    /* Watch for invalidation events. */
    CacheRegisterThreadRelcacheCallback(logicalrep_relmap_invalidate_cb, (Datum)0);
}

/*
 * Free the entry of a relation map cache.
 */
static void logicalrep_relmap_free_entry(LogicalRepRelMapEntry *entry)
{
    LogicalRepRelation *remoterel;

    remoterel = &entry->remoterel;

    pfree(remoterel->nspname);
    pfree(remoterel->relname);

    if (remoterel->natts > 0) {
        int i;

        for (i = 0; i < remoterel->natts; i++)
            pfree(remoterel->attnames[i]);

        pfree(remoterel->attnames);
        pfree(remoterel->atttyps);
    }

    bms_free(remoterel->attkeys);

    if (entry->attrmap)
        pfree(entry->attrmap);
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
 */
void logicalrep_relmap_update(LogicalRepRelation *remoterel)
{
    MemoryContext oldctx;
    LogicalRepRelMapEntry *entry;
    bool found;
    int i;
    int rc;

    if (t_thrd.applyworker_cxt.logicalRepRelMap == NULL)
        logicalrep_relmap_init();

    /*
     * HASH_ENTER returns the existing entry if present or creates a new one.
     */
    entry = (LogicalRepRelMapEntry *)hash_search(t_thrd.applyworker_cxt.logicalRepRelMap,
                                                 (void *)&remoterel->remoteid,
                                                 HASH_ENTER, &found);

    if (found)
        logicalrep_relmap_free_entry(entry);

    rc = memset_s(entry, sizeof(LogicalRepRelMapEntry), 0, sizeof(LogicalRepRelMapEntry));
    securec_check(rc, "", "");

    /* Make cached copy of the data */
    oldctx = MemoryContextSwitchTo(t_thrd.applyworker_cxt.logicalRepRelMapContext);
    entry->remoterel.remoteid = remoterel->remoteid;
    entry->remoterel.nspname = pstrdup(remoterel->nspname);
    entry->remoterel.relname = pstrdup(remoterel->relname);
    entry->remoterel.natts = remoterel->natts;
    entry->remoterel.attnames = (char **)palloc(remoterel->natts * sizeof(char *));
    entry->remoterel.atttyps = (Oid *)palloc(remoterel->natts * sizeof(Oid));
    for (i = 0; i < remoterel->natts; i++) {
        entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
        entry->remoterel.atttyps[i] = remoterel->atttyps[i];
    }
    entry->remoterel.replident = remoterel->replident;
    entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
    MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 *
 * Returns -1 if not found.
 */
static int logicalrep_rel_att_by_name(LogicalRepRelation *remoterel, const char *attname)
{
    int i;

    for (i = 0; i < remoterel->natts; i++) {
        if (strcmp(remoterel->attnames[i], attname) == 0)
            return i;
    }

    return -1;
}

/*
 * Open the local relation associated with the remote one.
 *
 * Rebuilds the Relcache mapping if it was invalidated by local DDL.
 */
LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
    LogicalRepRelMapEntry *entry;
    bool hashfound;
    LogicalRepRelation *remoterel = NULL;

    if (t_thrd.applyworker_cxt.logicalRepRelMap == NULL)
        logicalrep_relmap_init();

    /* Search for existing entry. */
    entry = (LogicalRepRelMapEntry *)hash_search(t_thrd.applyworker_cxt.logicalRepRelMap,
                                                 (void *)&remoteid,
                                                 HASH_FIND,
                                                 &hashfound);

    if (!hashfound)
        elog(ERROR, "no relation map entry for remote relation ID %u", remoteid);

    remoterel = &entry->remoterel;

    /* Ensure we don't leak a relcache refcount. */
    if (entry->localrel)
        elog(ERROR, "remote relation ID %u is already open", remoteid);

    /*
     * When opening and locking a relation, pending invalidation messages are
     * processed which can invalidate the relation.  Hence, if the entry is
     * currently considered valid, try to open the local relation by OID and
     * see if invalidation ensues.
     */
    if (entry->localrelvalid) {
        entry->localrel = try_relation_open(entry->localreloid, lockmode);
        if (!entry->localrel) {
            /* Table was renamed or dropped. */
            entry->localrelvalid = false;
        }  else if (!entry->localrelvalid) {
            /* Note we release the no-longer-useful lock here. */
            heap_close(entry->localrel, lockmode);
            entry->localrel = NULL;
        }
    }

    /*
     * If the entry has been marked invalid since we last had lock on it,
     * re-open the local relation by name and rebuild all derived data.
     */
    if (!entry->localrelvalid) {
        Oid relid;
        int found;
        Bitmapset *idkey;
        TupleDesc desc;
        MemoryContext oldctx;
        int i;

        /* Try to find and lock the relation by name. */
        relid = RangeVarGetRelid(makeRangeVar(remoterel->nspname, remoterel->relname, -1), lockmode, true);
        if (!OidIsValid(relid))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("logical replication target relation \"%s.%s\" does not exist", remoterel->nspname,
                remoterel->relname)));
        entry->localrel = heap_open(relid, NoLock);
        entry->localreloid = relid;
        remoterel = &entry->remoterel;

        /*
         * We currently only support writing to regular and partitioned
         * tables.
         */
        if (entry->localrel->rd_rel->relkind != RELKIND_RELATION)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("logical replication target relation \"%s.%s\" is not a table", remoterel->nspname,
                remoterel->relname)));

        /*
         * Build the mapping of local attribute numbers to remote attribute
         * numbers and validate that we don't miss any replicated columns
         * as that would result in potentially unwanted data loss.
         */
        desc = RelationGetDescr(entry->localrel);
        oldctx = MemoryContextSwitchTo(t_thrd.applyworker_cxt.logicalRepRelMapContext);
        entry->attrmap = (AttrNumber *)palloc(desc->natts * sizeof(AttrNumber));
        MemoryContextSwitchTo(oldctx);

        found = 0;
        for (i = 0; i < desc->natts; i++) {
            if (desc->attrs[i].attisdropped || GetGeneratedCol(desc, i)) {
                entry->attrmap[i] = -1;
                continue;
            }
            int attnum = logicalrep_rel_att_by_name(remoterel, NameStr(desc->attrs[i].attname));
            entry->attrmap[i] = attnum;
            if (attnum >= 0)
                found++;
        }

        /* detail message with names of missing columns */
        if (found < remoterel->natts)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("logical replication target relation \"%s.%s\" is missing "
                "some replicated columns",
                remoterel->nspname, remoterel->relname)));

        /*
         * Check that replica identity matches. We allow for stricter replica
         * identity (fewer columns) on subscriber as that will not stop us
         * from finding unique tuple. IE, if publisher has identity
         * (id,timestamp) and subscriber just (id) this will not be a problem,
         * but in the opposite scenario it will.
         *
         * Don't throw any error here just mark the relation entry as not
         * updatable, as replica identity is only for updates and deletes
         * but inserts can be replicated even without it.
         */
        entry->updatable = true;
        idkey = RelationGetIndexAttrBitmap(entry->localrel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
        /* fallback to PK if no replica identity */
        if (idkey == NULL) {
            idkey = RelationGetIndexAttrBitmap(entry->localrel, INDEX_ATTR_BITMAP_PRIMARY_KEY);
            /*
             * If no replica identity index and no PK, the published table
             * must have replica identity FULL.
             */
            if (idkey == NULL && remoterel->replident != REPLICA_IDENTITY_FULL)
                entry->updatable = false;
        }

        i = -1;
        while ((i = bms_next_member(idkey, i)) >= 0) {
            int attnum = i + FirstLowInvalidHeapAttributeNumber;

            if (!AttrNumberIsForUserDefinedAttr(attnum)) {
                continue;
            }

            attnum = AttrNumberGetAttrOffset(attnum);
            if (entry->attrmap[attnum] < 0 || !bms_is_member(entry->attrmap[attnum], remoterel->attkeys)) {
                entry->updatable = false;
                break;
            }
        }

        entry->localrelvalid = true;
    }

    if (entry->state != SUBREL_STATE_READY)
        entry->state = GetSubscriptionRelState(t_thrd.applyworker_cxt.mySubscription->oid,
                                               entry->localreloid,
                                               &entry->statelsn);

    return entry;
}

/*
 * Close the previously opened logical relation.
 */
void logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
{
    heap_close(rel->localrel, lockmode);
    rel->localrel = NULL;
}

