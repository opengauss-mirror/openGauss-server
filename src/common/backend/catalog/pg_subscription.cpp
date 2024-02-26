/* -------------------------------------------------------------------------
 *
 * pg_subscription.c
 * 		replication subscriptions
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		src/backend/catalog/pg_subscription.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tableam.h"

#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "storage/lmgr.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"
#include "catalog/indexing.h"
#include "utils/snapmgr.h"

static List *textarray_to_stringlist(ArrayType *textarray);

/*
 * Fetch the subscription from the syscache.
 */
Subscription *GetSubscription(Oid subid, bool missing_ok)
{
    HeapTuple tup;
    Subscription *sub;
    Form_pg_subscription subform;
    Datum datum;
    bool isnull;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
    if (!HeapTupleIsValid(tup)) {
        if (missing_ok) {
            return NULL;
        }
        elog(ERROR, "cache lookup failed for subscription %u", subid);
    }

    subform = (Form_pg_subscription)GETSTRUCT(tup);

    sub = (Subscription *)palloc(sizeof(Subscription));
    sub->oid = subid;
    sub->dbid = subform->subdbid;
    sub->name = pstrdup(NameStr(subform->subname));
    sub->owner = subform->subowner;
    sub->enabled = subform->subenabled;

    /* Get conninfo */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subconninfo, &isnull);
    if (unlikely(isnull)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conninfo for subscription %u", subid)));
    }
    sub->conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subslotname, &isnull);
    if (!isnull) {
        sub->slotname = pstrdup(NameStr(*DatumGetName(datum)));
    } else {
        sub->slotname = NULL;
    }

    /* Get synccommit */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subsynccommit, &isnull);
    if (unlikely(isnull)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("null synccommit for subscription %u", subid)));
    }
    sub->synccommit = TextDatumGetCString(datum);

    /* Get publications */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subpublications, &isnull);
    if (unlikely(isnull)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("null publications for subscription %u", subid)));
    }
    sub->publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subbinary, &isnull);
    if (unlikely(isnull)) {
        sub->binary = false;
    } else {
        sub->binary = DatumGetBool(datum);
    }

    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_submatchddlowner, &isnull);
    if (unlikely(isnull)) {
        /* default matchddlowner is true */
        sub->matchddlowner = true;
    } else {
        sub->matchddlowner = DatumGetBool(datum);
    }

    /* Get skiplsn */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subskiplsn, &isnull);
    if (unlikely(isnull)) {
        sub->skiplsn = InvalidXLogRecPtr;
    } else {
        sub->skiplsn = TextDatumGetLsn(datum);
    }

    ReleaseSysCache(tup);

    return sub;
}

/*
 * Return number of subscriptions defined in given database.
 * Used by dropdb() to check if database can indeed be dropped.
 */
int CountDBSubscriptions(Oid dbid)
{
    int nsubs = 0;
    Relation rel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple tup;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    ScanKeyInit(&scankey, Anum_pg_subscription_subdbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dbid));

    scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &scankey);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
        nsubs++;

    systable_endscan(scan);

    heap_close(rel, NoLock);

    return nsubs;
}

/*
 * Free memory allocated by subscription struct.
 */
void FreeSubscription(Subscription *sub)
{
    pfree(sub->synccommit);
    pfree(sub->name);
    pfree(sub->conninfo);
    if (sub->slotname) {
        pfree(sub->slotname);
    }
    list_free_deep(sub->publications);
    pfree(sub);
}

/*
 * get_subscription_oid - given a subscription name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid get_subscription_oid(const char *subname, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(subname));
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription \"%s\" does not exist", subname)));
    return oid;
}

/*
 * get_subscription_name - given a subscription OID, look up the name
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return NULL.
 */
char *get_subscription_name(Oid subid, bool missing_ok)
{
    HeapTuple tup;
    char *subname;
    Form_pg_subscription subform;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for subscription %u", subid);
        return NULL;
    }

    subform = (Form_pg_subscription)GETSTRUCT(tup);
    subname = pstrdup(NameStr(subform->subname));

    ReleaseSysCache(tup);

    return subname;
}

/* Clear the list content, only deal with DefElem and string content */
void ClearListContent(List *list)
{
    ListCell *cell = NULL;
    foreach(cell, list) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (def->arg == NULL || !IsA(def->arg, String)) {
            continue;
        }

        char *str = strVal(def->arg);
        if (str == NULL || str[0] == '\0') {
            continue;
        }

        size_t len = strlen(str);
        errno_t errCode = memset_s(str, len, 0, len);
        securec_check(errCode, "\0", "\0");
    }
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list of strings is pallocated here.
 */
static List *textarray_to_stringlist(ArrayType *textarray)
{
    Datum *elems;
    int nelems, i;
    List *res = NIL;

    deconstruct_array(textarray, TEXTOID, -1, false, 'i', &elems, NULL, &nelems);

    if (nelems == 0)
        return NIL;

    for (i = 0; i < nelems; i++)
        res = lappend(res, makeString(TextDatumGetCString(elems[i])));

    return res;
}

Datum LsnGetTextDatum(XLogRecPtr lsn)
{
    char clsn[MAXFNAMELEN];
    int ret = snprintf_s(clsn, sizeof(clsn), sizeof(clsn) - 1, "%X/%X", (uint32)(lsn >> 32), (uint32)lsn);
    securec_check_ss(ret, "\0", "\0");

    return CStringGetTextDatum(clsn);
}

XLogRecPtr TextDatumGetLsn(Datum datum)
{
    XLogRecPtr lsn;
    uint32  lsn_hi;
    uint32  lsn_lo;
    char* clsn = TextDatumGetCString(datum);
    int ret = sscanf_s(clsn, "%X/%X", &lsn_hi, &lsn_lo);
    securec_check_for_sscanf_s(ret, 2, "\0", "\0");
    /* Calculate LSN */
    lsn = ((uint64) lsn_hi )<< 32 | lsn_lo;

    return lsn;
}

/*
 * Add new state record for a subscription table.
 */
Oid AddSubscriptionRelState(Oid subid, Oid relid, char state)
{
    Relation rel;
    HeapTuple tup;
    Oid subrelid;
    bool nulls[Natts_pg_subscription_rel];
    Datum values[Natts_pg_subscription_rel];
    int rc;

    /* Prevent concurrent changes. */
    rel = heap_open(SubscriptionRelRelationId, ShareRowExclusiveLock);

    /* Try finding existing mapping. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP, ObjectIdGetDatum(relid), ObjectIdGetDatum(subid));

    if (HeapTupleIsValid(tup))
        elog(ERROR, "subscription table %u in subscription %u already exists", relid, subid);

    /* Form the tuple. */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    values[Anum_pg_subscription_rel_srsubid - 1] = ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_rel_srrelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);
    values[Anum_pg_subscription_rel_srcsn - 1] = UInt64GetDatum(InvalidCommitSeqNo);
    values[Anum_pg_subscription_rel_srsublsn - 1] = LsnGetTextDatum(InvalidXLogRecPtr);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    subrelid = simple_heap_insert(rel, tup);
    CatalogUpdateIndexes(rel, tup);
    tableam_tops_free_tuple(tup);
    /* Cleanup. */
    heap_close(rel, NoLock);

    return subrelid;
}

/*
 * Update the state of a subscription table.
 */
Oid UpdateSubscriptionRelState(Oid subid, Oid relid, char state, XLogRecPtr sublsn, CommitSeqNo subcsn)
{
    Relation rel;
    HeapTuple tup;
    Oid subrelid;
    bool nulls[Natts_pg_subscription_rel];
    Datum values[Natts_pg_subscription_rel];
    bool replaces[Natts_pg_subscription_rel];
    int rc;

    LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

    rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

    /* Try finding existing mapping. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP, ObjectIdGetDatum(relid), ObjectIdGetDatum(subid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "subscription table %u in subscription %u does not exist", relid, subid);

    /* Update the tuple. */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "", "");

    replaces[Anum_pg_subscription_rel_srsubstate - 1] = true;
    values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);

    if (subcsn != InvalidCommitSeqNo) {
        replaces[Anum_pg_subscription_rel_srcsn - 1] = true;
        values[Anum_pg_subscription_rel_srcsn - 1] = Int64GetDatum(subcsn);
    }

    replaces[Anum_pg_subscription_rel_srsublsn - 1] = true;
    if (sublsn != InvalidXLogRecPtr)
        values[Anum_pg_subscription_rel_srsublsn - 1] = LsnGetTextDatum(sublsn);
    else
        nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;

    tup = (HeapTuple)tableam_tops_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    /* Update the catalog. */
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    subrelid = HeapTupleGetOid(tup);
    tableam_tops_free_tuple(tup);

    /* Cleanup. */
    heap_close(rel, NoLock);

    return subrelid;
}

/*
 * Get state of subscription table.
 *
 * Returns SUBREL_STATE_UNKNOWN when the table is not in the subscription.
 */
char GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn, CommitSeqNo *subcsn)
{
    HeapTuple tup;
    char substate;
    bool isnull;
    Datum d;
    Relation rel;

    /*
     * This is to avoid the race condition with AlterSubscription which tries
     * to remove this relstate.
     */
    rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

    /* Try finding the mapping. */
    tup = SearchSysCache2(SUBSCRIPTIONRELMAP, ObjectIdGetDatum(relid), ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup)) {
        heap_close(rel, AccessShareLock);
        *sublsn = InvalidXLogRecPtr;
        return SUBREL_STATE_UNKNOWN;
    }

    /* Get the state. */
    substate = ((Form_pg_subscription_rel)GETSTRUCT(tup))->srsubstate;

    /* Get the LSN */
    d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup, Anum_pg_subscription_rel_srsublsn, &isnull);
    if (isnull)
        *sublsn = InvalidXLogRecPtr;
    else
        *sublsn = TextDatumGetLsn(d);

    if (subcsn) {
        /* Get the Csn */
        d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup, Anum_pg_subscription_rel_srcsn, &isnull);
        if (isnull)
            *subcsn = InvalidCommitSeqNo;
        else
            *subcsn = DatumGetInt64(d);
    }

    /* Cleanup */
    ReleaseSysCache(tup);

    heap_close(rel, AccessShareLock);

    return substate;
}

/*
 * Drop subscription relation mapping. These can be for a particular
 * subscription, or for a particular relation, or both.
 */
void RemoveSubscriptionRel(Oid subid, Oid relid)
{
    Relation rel;
    TableScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple tup;
    int nkeys = 0;

    /* Prevent concurrent changes (see SetSubscriptionRelState()). */
    rel = heap_open(SubscriptionRelRelationId, ShareRowExclusiveLock);

    if (OidIsValid(subid)) {
        ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srsubid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(subid));
    }

    if (OidIsValid(relid)) {
        ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srrelid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(relid));
    }

    /* Do the search and delete what we found. */
    scan = tableam_scan_begin(rel, SnapshotNow, nkeys, skey);
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_subscription_rel subrel = (Form_pg_subscription_rel)GETSTRUCT(tup);

        /*
         * We don't allow to drop the relation mapping when the table
         * synchronization is in progress unless the caller updates the
         * corresponding subscription as well. This is to ensure that we don't
         * leave tablesync slots or origins in the system when the
         * corresponding table is dropped.
         */
        if (!OidIsValid(subid) && subrel->srsubstate != SUBREL_STATE_READY) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not drop relation mapping for subscription \"%s\"",
                    get_subscription_name(subrel->srsubid, false)),
                errdetail("Table synchronization for relation \"%s\" is in progress and is in state \"%c\".",
                    get_rel_name(relid), subrel->srsubstate),
                /*
                 * translator: first %s is a SQL ALTER command and second %s is a
                 * SQL DROP command
                 */
                errhint("Use %s to enable subscription if not already enabled or use %s to drop the subscription.",
                "ALTER SUBSCRIPTION ... ENABLE", "DROP SUBSCRIPTION ...")));
        }

        simple_heap_delete(rel, &tup->t_self);
    }
    heap_endscan(scan);

    heap_close(rel, ShareRowExclusiveLock);
}

/*
 * Get all relations for subscription, or get that are
 * not in a ready status if needNotReady is true.
 *
 * Returned list is palloced in current memory context.
 */
List *GetSubscriptionRelations(Oid subid, bool needNotReady)
{
    List *res = NIL;
    Relation rel;
    HeapTuple tup;
    int nkeys = 0;
    ScanKeyData skey[2];
    SysScanDesc scan;

    rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

    ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srsubid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(subid));

    if (needNotReady) {
        ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srsubstate, BTEqualStrategyNumber, F_CHARNE,
            CharGetDatum(SUBREL_STATE_READY));
    }

    scan = systable_beginscan(rel, InvalidOid, false, NULL, nkeys, skey);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_subscription_rel subrel;
        SubscriptionRelState *relstate;
        Datum d;
        bool isnull;

        subrel = (Form_pg_subscription_rel)GETSTRUCT(tup);

        relstate = (SubscriptionRelState *)palloc(sizeof(SubscriptionRelState));
        relstate->relid = subrel->srrelid;
        relstate->state = subrel->srsubstate;
        d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup, Anum_pg_subscription_rel_srsublsn, &isnull);
        if (isnull)
            relstate->lsn = InvalidXLogRecPtr;
        else
            relstate->lsn = TextDatumGetLsn(d);

        res = lappend(res, relstate);
    }

    /* Cleanup */
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return res;
}
