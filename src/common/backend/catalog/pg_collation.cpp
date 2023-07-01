/* -------------------------------------------------------------------------
 *
 * pg_collation.cpp
 *	  routines to support manipulation of the pg_collation relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_collation.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_fn.h"
#include "catalog/pg_namespace.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "lib/string.h"

/*
 * CollationCreate
 *
 * Add a new tuple to pg_collation.
 */
Oid CollationCreate(const char* collname, Oid collnamespace, Oid collowner, int32 collencoding, const char* collcollate,
    const char* collctype)
{
    Relation rel;
    TupleDesc tupDesc;
    HeapTuple tup;
    Datum values[Natts_pg_collation];
    bool nulls[Natts_pg_collation];
    NameData name_name, name_collate, name_ctype;
    Oid oid;
    ObjectAddress myself, referenced;

    AssertArg(collname);
    AssertArg(collnamespace);
    AssertArg(collowner);
    AssertArg(collcollate);
    AssertArg(collctype);

    /*
     * Make sure there is no existing collation of same name & encoding.
     *
     * This would be caught by the unique index anyway; we're just giving a
     * friendlier error message.  The unique index provides a backstop against
     * race conditions.
     */
    if (SearchSysCacheExists3(
            COLLNAMEENCNSP, PointerGetDatum(collname), Int32GetDatum(collencoding), ObjectIdGetDatum(collnamespace)))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("collation \"%s\" for encoding \"%s\" already exists",
                    collname,
                    pg_encoding_to_char(collencoding))));

    /*
     * Also forbid matching an any-encoding entry.	This test of course is not
     * backed up by the unique index, but it's not a problem since we don't
     * support adding any-encoding entries after initdb.
     */
    if (SearchSysCacheExists3(
            COLLNAMEENCNSP, PointerGetDatum(collname), Int32GetDatum(-1), ObjectIdGetDatum(collnamespace)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("collation \"%s\" already exists", collname)));

    /* open pg_collation */
    rel = heap_open(CollationRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* form a tuple */
    errno_t rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");

    (void)namestrcpy(&name_name, collname);
    values[Anum_pg_collation_collname - 1] = NameGetDatum(&name_name);
    values[Anum_pg_collation_collnamespace - 1] = ObjectIdGetDatum(collnamespace);
    values[Anum_pg_collation_collowner - 1] = ObjectIdGetDatum(collowner);
    values[Anum_pg_collation_collencoding - 1] = Int32GetDatum(collencoding);
    (void)namestrcpy(&name_collate, collcollate);
    values[Anum_pg_collation_collcollate - 1] = NameGetDatum(&name_collate);
    (void)namestrcpy(&name_ctype, collctype);
    values[Anum_pg_collation_collctype - 1] = NameGetDatum(&name_ctype);

    tup = heap_form_tuple(tupDesc, values, nulls);

    /* insert a new tuple */
    oid = simple_heap_insert(rel, tup);
    Assert(OidIsValid(oid));

    /* update the index if any */
    CatalogUpdateIndexes(rel, tup);

    /* set up dependencies for the new collation */
    myself.classId = CollationRelationId;
    myself.objectId = oid;
    myself.objectSubId = 0;

    /* create dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = collnamespace;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on owner */
    recordDependencyOnOwner(CollationRelationId, HeapTupleGetOid(tup), collowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new collation */
    InvokeObjectAccessHook(OAT_POST_CREATE, CollationRelationId, oid, 0, NULL);

    heap_freetuple_ext(tup);
    heap_close(rel, RowExclusiveLock);

    return oid;
}

/*
 * RemoveCollationById
 *
 * Remove a tuple from pg_collation by Oid. This function is solely
 * called inside catalog/dependency.c
 */
void RemoveCollationById(Oid collationOid)
{
    Relation rel;
    ScanKeyData scanKeyData;
    SysScanDesc scandesc;
    HeapTuple tuple;

    rel = heap_open(CollationRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKeyData, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(collationOid));

    scandesc = systable_beginscan(rel, CollationOidIndexId, true, NULL, 1, &scanKeyData);

    tuple = systable_getnext(scandesc);

    if (HeapTupleIsValid(tuple))
        simple_heap_delete(rel, &tuple->t_self);
    else
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("could not find tuple for collation %u", collationOid)));

    systable_endscan(scandesc);

    heap_close(rel, RowExclusiveLock);
}

int get_charset_by_collation(Oid coll_oid)
{
    HeapTuple tp = NULL;
    int result = PG_INVALID_ENCODING;

    /* The collation OID in B format has a rule, through which we can quickly get the charset from the OID. */
    if (COLLATION_IN_B_FORMAT(coll_oid)) {
        return FAST_GET_CHARSET_BY_COLL(coll_oid);
    }
    
    if (COLLATION_HAS_INVALID_ENCODING(coll_oid)) {
        return result;
    }

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(coll_oid));
    if (!HeapTupleIsValid(tp)) {
        return result;
    }
    Form_pg_collation coll_tup = (Form_pg_collation)GETSTRUCT(tp);
    result = coll_tup->collencoding;
    ReleaseSysCache(tp);
    return result;
}

int get_valid_charset_by_collation(Oid coll_oid)
{
    if (!DB_IS_CMPT(B_FORMAT)) {
        return GetDatabaseEncoding();
    }
    int charset = get_charset_by_collation(coll_oid);
    if (charset == PG_INVALID_ENCODING) {
        return GetDatabaseEncoding();
    }
    return charset;
}

Oid get_default_collation_by_charset(int charset, bool report_error)
{
    Oid coll_oid = InvalidOid;
    Relation rel;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;

    rel = heap_open(CollationRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_collation_collencoding, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(charset));
    ScanKeyInit(&key[1], Anum_pg_collation_collisdef, BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));

    scan = systable_beginscan(rel, CollationEncDefIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        coll_oid = HeapTupleGetOid(tup);
        break;
    }
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (coll_oid == InvalidOid && report_error) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("default collation for encoding \"%s\" does not exist",
                    pg_encoding_to_char(charset))));
    }
    return coll_oid;
}