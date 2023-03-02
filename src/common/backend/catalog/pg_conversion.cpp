/* -------------------------------------------------------------------------
 *
 * pg_conversion.cpp
 *	  routines to support manipulation of the pg_conversion relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_conversion.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * ConversionCreate
 *
 * Add a new tuple to pg_conversion.
 */
ObjectAddress ConversionCreate(const char* conname, Oid connamespace, Oid conowner, int32 conforencoding, int32 contoencoding,
    Oid conproc, bool def)
{
    int i;
    Relation rel = NULL;
    TupleDesc tupDesc = NULL;
    HeapTuple tup = NULL;
    bool nulls[Natts_pg_conversion];
    Datum values[Natts_pg_conversion];
    NameData cname;
    Oid oid;
    ObjectAddress myself, referenced;

    /* sanity checks */
    if (conname == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("no conversion name supplied")));

    /* make sure there is no existing conversion of same name */
    if (SearchSysCacheExists2(CONNAMENSP, PointerGetDatum(conname), ObjectIdGetDatum(connamespace)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("conversion \"%s\" already exists", conname)));

    if (def) {
        /*
         * make sure there is no existing default <for encoding><to encoding>
         * pair in this name space
         */
        if (FindDefaultConversion(connamespace, conforencoding, contoencoding))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("default conversion for %s to %s already exists",
                        pg_encoding_to_char(conforencoding),
                        pg_encoding_to_char(contoencoding))));
    }

    /* open pg_conversion */
    rel = heap_open(ConversionRelationId, RowExclusiveLock);
    tupDesc = rel->rd_att;

    /* initialize nulls and values */
    for (i = 0; i < Natts_pg_conversion; i++) {
        nulls[i] = false;
        values[i] = (Datum)NULL;
    }

    /* form a tuple */
    (void)namestrcpy(&cname, conname);
    values[Anum_pg_conversion_conname - 1] = NameGetDatum(&cname);
    values[Anum_pg_conversion_connamespace - 1] = ObjectIdGetDatum(connamespace);
    values[Anum_pg_conversion_conowner - 1] = ObjectIdGetDatum(conowner);
    values[Anum_pg_conversion_conforencoding - 1] = Int32GetDatum(conforencoding);
    values[Anum_pg_conversion_contoencoding - 1] = Int32GetDatum(contoencoding);
    values[Anum_pg_conversion_conproc - 1] = ObjectIdGetDatum(conproc);
    values[Anum_pg_conversion_condefault - 1] = BoolGetDatum(def);

    tup = heap_form_tuple(tupDesc, values, nulls);

    /* insert a new tuple */
    oid = simple_heap_insert(rel, tup);
    Assert(OidIsValid(oid));

    /* update the index if any */
    CatalogUpdateIndexes(rel, tup);

    myself.classId = ConversionRelationId;
    myself.objectId = HeapTupleGetOid(tup);
    myself.objectSubId = 0;

    /* create dependency on conversion procedure */
    referenced.classId = ProcedureRelationId;
    referenced.objectId = conproc;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = connamespace;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on owner */
    recordDependencyOnOwner(ConversionRelationId, HeapTupleGetOid(tup), conowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new conversion */
    InvokeObjectAccessHook(OAT_POST_CREATE, ConversionRelationId, HeapTupleGetOid(tup), 0, NULL);

    heap_freetuple_ext(tup);
    heap_close(rel, RowExclusiveLock);

    return myself;
}

/*
 * RemoveConversionById
 *
 * Remove a tuple from pg_conversion by Oid. This function is solely
 * called inside catalog/dependency.c
 */
void RemoveConversionById(Oid conversionOid)
{
    Relation rel = NULL;
    HeapTuple tuple = NULL;
    TableScanDesc scan = NULL;
    ScanKeyData scanKeyData;

    ScanKeyInit(&scanKeyData, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(conversionOid));

    /* open pg_conversion */
    rel = heap_open(ConversionRelationId, RowExclusiveLock);

    scan = heap_beginscan(rel, SnapshotNow, 1, &scanKeyData);

    /* search for the target tuple */
    if (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
        simple_heap_delete(rel, &tuple->t_self);
    else
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("could not find tuple for conversion %u", conversionOid)));
    heap_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}

/*
 * FindDefaultConversion
 *
 * Find "default" conversion proc by for_encoding and to_encoding in the
 * given namespace.
 *
 * If found, returns the procedure's oid, otherwise InvalidOid.  Note that
 * you get the procedure's OID not the conversion's OID!
 */
Oid FindDefaultConversion(Oid name_space, int32 for_encoding, int32 to_encoding)
{
    CatCList* catlist = NULL;
    HeapTuple tuple = NULL;
    Form_pg_conversion body = NULL;
    Oid proc = InvalidOid;
    int i;

    catlist = SearchSysCacheList3(
        CONDEFAULT, ObjectIdGetDatum(name_space), Int32GetDatum(for_encoding), Int32GetDatum(to_encoding));

    for (i = 0; i < catlist->n_members; i++) {
        tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        body = (Form_pg_conversion)GETSTRUCT(tuple);
        if (body->condefault) {
            proc = body->conproc;
            break;
        }
    }
    ReleaseSysCacheList(catlist);
    return proc;
}
