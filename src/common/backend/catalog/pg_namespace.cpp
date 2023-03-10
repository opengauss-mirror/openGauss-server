/* -------------------------------------------------------------------------
 *
 * pg_namespace.cpp
 *	  routines to support manipulation of the pg_namespace relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_namespace.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_control.h"
#include "catalog/pg_namespace.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "catalog/pg_collation.h"

/* ----------------
 * NamespaceCreate
 *
 * Create a namespace (schema) with the given name and owner OID.
 *
 * If isTemp is true, this schema is a per-backend schema for holding
 * temporary tables.  Currently, the only effect of that is to prevent it
 * from being linked as a member of any active extension.  (If someone
 * does CREATE TEMP TABLE in an extension script, we don't want the temp
 * schema to become part of the extension.)
 * ---------------
 */
Oid NamespaceCreate(const char* nspName, Oid ownerId, bool isTemp, bool hasBlockChain, Oid colloid)
{
    Relation nspdesc;
    HeapTuple tup;
    Oid nspoid;
    bool nulls[Natts_pg_namespace];
    Datum values[Natts_pg_namespace];
    NameData nname;
    TupleDesc tupDesc;
    ObjectAddress myself;
    int i;

    /* sanity checks */
    if (nspName == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no namespace name supplied")));

    /* make sure there is no existing namespace of same name */
    if (SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(nspName)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("schema \"%s\" already exists", nspName)));

    /* initialize nulls and values */
    for (i = 0; i < Natts_pg_namespace; i++) {
        nulls[i] = false;
        values[i] = (Datum)NULL;
    }
    (void)namestrcpy(&nname, nspName);
    values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&nname);
    values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_namespace_nspblockchain - 1] = hasBlockChain;
    if (IS_PGXC_DATANODE && !isSingleMode && !isRestoreMode && !u_sess->attr.attr_common.xc_maintenance_mode &&
        (isTempNamespaceName(nspName) || isToastTempNamespaceName(nspName)))
        values[Anum_pg_namespace_nsptimeline - 1] = get_controlfile_timeline();
    else
        values[Anum_pg_namespace_nsptimeline - 1] = 0; /* the controlfile_timeline is at least 1, so 0 is invalid */
    nulls[Anum_pg_namespace_nspacl - 1] = true;

    values[Anum_pg_namespace_in_redistribution - 1] = 'n';
    if (colloid != InvalidOid) {
        values[Anum_pg_namespace_nspcollation - 1] = colloid;
    } else {
        nulls[Anum_pg_namespace_nspcollation - 1] = true;
    }

    nspdesc = heap_open(NamespaceRelationId, RowExclusiveLock);
    tupDesc = nspdesc->rd_att;

    tup = heap_form_tuple(tupDesc, values, nulls);

    if (u_sess->attr.attr_common.IsInplaceUpgrade &&
        u_sess->upg_cxt.Inplace_upgrade_next_pg_namespace_oid != InvalidOid) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_pg_namespace_oid);
        u_sess->upg_cxt.Inplace_upgrade_next_pg_namespace_oid = InvalidOid;
    }

    nspoid = simple_heap_insert(nspdesc, tup);
    Assert(OidIsValid(nspoid));

    CatalogUpdateIndexes(nspdesc, tup);

    heap_close(nspdesc, RowExclusiveLock);

    /* Record dependencies */
    myself.classId = NamespaceRelationId;
    myself.objectId = nspoid;
    myself.objectSubId = 0;

    if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId)
        recordPinnedDependency(&myself);
    else {
        /* dependency on owner */
        recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);

        /* dependency on extension ... but not for magic temp schemas */
        if (!isTemp)
            recordDependencyOnCurrentExtension(&myself, false);
    }

    /* Post creation hook for new schema */
    InvokeObjectAccessHook(OAT_POST_CREATE, NamespaceRelationId, nspoid, 0, NULL);

    return nspoid;
}

bool IsLedgerNameSpace(Oid nspOid)
{
    if (!OidIsValid(nspOid)) {
        return false;
    }
    HeapTuple tuple;
    bool is_nspblockchain = false;

    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspOid));
    if (HeapTupleIsValid(tuple)) {
        bool is_null = true;
        Datum datum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspblockchain, &is_null);
        if (!is_null) {
            is_nspblockchain = DatumGetBool(datum);
        }
        ReleaseSysCache(tuple);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema of oid \"%u\" does not exist", nspOid)));
    }
    return is_nspblockchain;
}

Oid get_nsp_default_collation(Oid nsp_oid)
{
    Oid nsp_def_coll = InvalidOid;
    HeapTuple tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
    if (HeapTupleIsValid(tp)) {
        bool is_null = true;
        Datum datum = SysCacheGetAttr(NAMESPACEOID, tp, Anum_pg_namespace_nspcollation, &is_null);
        if (!is_null) {
            nsp_def_coll = DatumGetObjectId(datum);
        }
        ReleaseSysCache(tp);
    }
    return nsp_def_coll;
}