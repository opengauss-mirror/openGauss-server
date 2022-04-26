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

#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

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
