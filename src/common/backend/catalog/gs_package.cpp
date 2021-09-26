/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * gs_package.cpp
 *     Definition about catalog of package.
 * 
 * 
 * IDENTIFICATION
 *        src/common/backend/catalog/gs_package.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_control.h"
#include "catalog/pg_namespace.h"
#include "catalog/gs_package.h"
#include "catalog/gs_package_fn.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "pgxc/pgxc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "tcop/utility.h"
#include "utils/plpgsql.h"

/* ----------------
 * PackageSpecCreate
 *
 * Create a package (package) with the given name and owner OID.
 *
 * If isTemp is true, this package is a per-backend package for holding
 * temporary tables.  Currently, the only effect of that is to prevent it
 * from being linked as a member of any active extension.  (If someone
 * does CREATE TEMP TABLE in an extension script, we don't want the temp
 * package to become part of the extension.)
 * ---------------
 */
Oid PackageNameGetOid(const char* pkgname, Oid namespaceId)
{

    if (OidIsValid(namespaceId)) {
        Oid pkgOid = InvalidOid;
        pkgOid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(pkgname), ObjectIdGetDatum(namespaceId));
        if (OidIsValid(pkgOid)) {
            return pkgOid;
        }
    } else {
        List* tempActiveSearchPath = NIL;
        Oid pkgOid;
        ListCell* l = NULL;

        recomputeNamespacePath();

        tempActiveSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, tempActiveSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            pkgOid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(pkgname), ObjectIdGetDatum(namespaceId));
            if (OidIsValid(pkgOid)) {
                list_free_ext(tempActiveSearchPath);
                return pkgOid;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }
    return InvalidOid;
}

bool IsExistPackageName(const char* pkgname)
{
    CatCList* catlist = NULL;
    catlist = SearchSysCacheList1(PKGNAMENSP, CStringGetDatum(pkgname));
    if (catlist != NULL) {
        if (catlist->n_members != 0) {
            ReleaseSysCacheList(catlist);
            return true;
        }
    }
    ReleaseSysCacheList(catlist);
    return false;
}

Oid PackageNameListGetOid(List* pkgnameList, bool missing_ok) 
{
    Oid pkgOid = InvalidOid;
    char* schemaname = NULL;
    char* pkgname = NULL;
    char* objname = NULL;
    DeconstructQualifiedName(pkgnameList, &schemaname, &objname, &pkgname);
    if (objname != NULL) {
        pkgname = objname;
    } else if (pkgname == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PACKAGE),
                errmsg("package %s does not exist", NameListToString(pkgnameList))));
    }
    if (schemaname != NULL) {
        Oid namespaceId = LookupExplicitNamespace(schemaname);
        pkgOid = PackageNameGetOid(pkgname, namespaceId);
    } else {
        pkgOid = PackageNameGetOid(pkgname);
    }
    if (!OidIsValid(pkgOid) && !missing_ok) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PACKAGE),
                errmsg("package %s does not exist", NameListToString(pkgnameList))));
    } else if (OidIsValid(pkgOid)) {
        return pkgOid;
    } else {
        return InvalidOid;
    }
    return pkgOid;
}

NameData* GetPackageName(Oid packageOid) 
{
    bool isNull = false;
    HeapTuple pkgTuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
    NameData* pkgName = NULL;
    if (HeapTupleIsValid(pkgTuple)) {
        Datum pkgName_datum = SysCacheGetAttr(PACKAGEOID, pkgTuple, Anum_gs_package_pkgname, &isNull);
        pkgName = DatumGetName(pkgName_datum);
        ReleaseSysCache(pkgTuple);
    } else {
        pkgName = NULL;
    }
    return pkgName;
}

PLpgSQL_package* PackageInstantiation(Oid packageOid)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                errmsg("not support create package in distributed database")));
#endif
    HeapTuple pkgTuple = NULL;
    bool pushed = false;
    bool isSpec = false;
    PLpgSQL_package* pkg = NULL;
    Datum packagebodydatum;
    bool isnull = false;
    pkgTuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
    if (!HeapTupleIsValid(pkgTuple)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while compile package", packageOid)));
    }
    packagebodydatum = SysCacheGetAttr(PROCOID, pkgTuple, Anum_gs_package_pkgbodydeclsrc, &isnull);
    if (isnull) {
        isSpec = true;
    } else {
        isSpec = false;
    }
    ReleaseSysCache(pkgTuple);

    pushed = SPI_push_conditional();

    pkg = plpgsql_package_validator(packageOid, isSpec);

    SPI_pop_conditional(pushed);

    return pkg;
}

Oid PackageSpecCreate(Oid pkgNamespace, const char* pkgName, const Oid ownerId, const char* pkgSpecSrc, bool replace)
{
    Relation pkgDesc;
    Oid pkgOid = InvalidOid;
    bool nulls[Natts_gs_package];
    Datum values[Natts_gs_package];
    bool replaces[Natts_gs_package];
    NameData nname;
    TupleDesc tupDesc;
    ObjectAddress myself;
    ObjectAddress referenced;
    int i;
    bool isReplaced = false;
    Assert(PointerIsValid(pkgSpecSrc));
    HeapTuple tup;
    HeapTuple oldpkgtup;
    Oid oldPkgOid;
    Acl* pkgacl = NULL;
    /* sanity checks */
    Oid schema_oid = get_namespace_oid(pkgName, true);
    if (OidIsValid(schema_oid)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("package can not create the same name with schema."),
                errdetail("same schema name exists: %s", pkgName),
                errcause("Package name conflict"),
                erraction("Please rename package name")));
    }
    /* initialize nulls and values */
    for (i = 0; i < Natts_gs_package; i++) {
        nulls[i] = false;
        values[i] = (Datum)NULL;
        replaces[i] = true;
    }
    (void)namestrcpy(&nname, pkgName);
    values[Anum_gs_package_pkgnamespace - 1] = ObjectIdGetDatum(pkgNamespace);
    values[Anum_gs_package_pkgname - 1] = NameGetDatum(&nname);
    values[Anum_gs_package_pkgowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_gs_package_pkgspecsrc - 1] = CStringGetTextDatum(pkgSpecSrc);
    nulls[Anum_gs_package_pkgbodydeclsrc - 1] = true;
    nulls[Anum_gs_package_pkgbodyinitsrc - 1] = true;
    nulls[Anum_gs_package_pkgacl - 1] = true;
    pkgDesc = heap_open(PackageRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(pkgDesc);

    tup = heap_form_tuple(tupDesc, values, nulls);

    oldPkgOid = PackageNameGetOid(pkgName, pkgNamespace);

    if (OidIsValid(oldPkgOid)) {
        if (replace != true) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("package \"%s\" already exists.", pkgName)));
        } else {
            oldpkgtup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(oldPkgOid));
            replaces[Anum_gs_package_pkgacl - 1] = false;
            if (!HeapTupleIsValid(oldpkgtup)) {
                ereport(ERROR,
                    (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for package %u, while compile package", oldPkgOid),
                        errdetail("cache lookup failed"),
                        errcause("System error"),
                        erraction("Drop and rebuild package.")));
            }
            tup = heap_modify_tuple(oldpkgtup, tupDesc, values, nulls, replaces);
            simple_heap_update(pkgDesc, &tup->t_self, tup); 
            ReleaseSysCache(oldpkgtup);
            isReplaced = true;
        } 
    } else {
        oldpkgtup = NULL;
        (void)simple_heap_insert(pkgDesc, tup);
        pkgacl = get_user_default_acl(ACL_OBJECT_PACKAGE, ownerId, pkgNamespace);
        if (pkgacl != NULL)
            values[Anum_gs_package_pkgacl - 1] = PointerGetDatum(pkgacl);
        else
            nulls[Anum_gs_package_pkgacl - 1] = true;
    }
    if (u_sess->attr.attr_common.IsInplaceUpgrade &&
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid != InvalidOid) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid);
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid = InvalidOid;
    }
    pkgOid = HeapTupleGetOid(tup);
    Assert(OidIsValid(pkgOid));

    CatalogUpdateIndexes(pkgDesc, tup);

    if (isReplaced) {
        (void)deleteDependencyRecordsFor(PackageRelationId, pkgOid, true);
        /* the 'shared dependencies' also change when update. */
        deleteSharedDependencyRecordsFor(PackageRelationId, pkgOid, 0); 
        dropFunctionByPackageOid(pkgOid);
    }  
    heap_freetuple_ext(tup);

    heap_close(pkgDesc, RowExclusiveLock);

    /* Record dependencies */
    myself.classId = PackageRelationId;
    myself.objectId = pkgOid;
    myself.objectSubId = 0;

    if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId && !isReplaced)
        recordPinnedDependency(&myself);
    else {
        /* dependency on owner */
        referenced.classId = NamespaceRelationId;
        referenced.objectId = pkgNamespace;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        recordDependencyOnOwner(PackageRelationId, pkgOid, ownerId);

        recordDependencyOnCurrentExtension(&myself, false);
    }

    /* Post creation hook for new schema */
    InvokeObjectAccessHook(OAT_POST_CREATE, PackageRelationId, pkgOid, 0, NULL);

    /* Advance command counter so new tuple can be seen by validator */
    CommandCounterIncrement();

        /* Recode the procedure create time. */
    if (OidIsValid(pkgOid)) {
            if (!isReplaced) {
                PgObjectOption objectOpt = {true, true, false, false};
                CreatePgObject(pkgOid, OBJECT_TYPE_PKGSPEC, ownerId, objectOpt);
            } else {
                UpdatePgObjectMtime(pkgOid, OBJECT_TYPE_PROC);
            }
    }
    plpgsql_package_validator(pkgOid, true, true);

    return pkgOid;
}

Oid PackageBodyCreate(Oid pkgNamespace, const char* pkgName, const Oid ownerId, const char* pkgBodySrc, const char* pkgInitSrc, bool replace)
{
    Relation pkgDesc;
    Oid pkgOid = InvalidOid;
    bool nulls[Natts_gs_package];
    Datum values[Natts_gs_package];
    bool replaces[Natts_gs_package];
    NameData nname;
    TupleDesc tupDesc;
    ObjectAddress myself;
    ObjectAddress referenced;
    int i = 0;
    bool isReplaced = false;
    Assert(PointerIsValid(pkgBodySrc));
    HeapTuple tup = NULL;
    HeapTuple oldpkgtup = NULL;
    Oid packageOid = InvalidOid;
    Oid oldPkgOid = InvalidOid;
    /* sanity checks */
    if (pkgName == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no package name supplied")));
    }
    Oid schema_oid = get_namespace_oid(pkgName, true);
    if (OidIsValid(schema_oid)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("package can not create the same name with schema."),
                errdetail("same schema name exists: %s", pkgName),
                errcause("Package name conflict"),
                erraction("Please rename package name")));

    }
    packageOid = PackageNameGetOid(pkgName, pkgNamespace);
    if (packageOid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("package spec not found")));
    }
    /* initialize nulls and values */
    for (i = 0; i < Natts_gs_package; i++) {
        nulls[i] = false;
        values[i] = (Datum)NULL;
        replaces[i] = false;
    }
    (void)namestrcpy(&nname, pkgName);
    values[Anum_gs_package_pkgbodydeclsrc - 1] = CStringGetTextDatum(pkgBodySrc);
    replaces[Anum_gs_package_pkgbodydeclsrc - 1] = true;
    if (pkgInitSrc == NULL) {
        nulls[Anum_gs_package_pkgbodyinitsrc - 1] = true;
        replaces[Anum_gs_package_pkgbodyinitsrc - 1] = true;
    } else {
        values[Anum_gs_package_pkgbodyinitsrc - 1] = CStringGetTextDatum(pkgInitSrc);
        replaces[Anum_gs_package_pkgbodyinitsrc - 1] = true;
    }
    pkgDesc = heap_open(PackageRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(pkgDesc);


    oldPkgOid = PackageNameGetOid(pkgName, pkgNamespace);
    if (OidIsValid(oldPkgOid)) {
        oldpkgtup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(oldPkgOid));
        if (!HeapTupleIsValid(oldpkgtup)) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for package %u, while compile package", oldPkgOid),
                    errdetail("cache lookup failed"),
                    errcause("System error"),
                    erraction("Drop and rebuild package")));
        }
        bool isNull = false;
        SysCacheGetAttr(PACKAGEOID, oldpkgtup, Anum_gs_package_pkgbodydeclsrc, &isNull);
        if (!isNull && !replace) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_PACKAGE), errmsg("package body already exists")));
        } 
        tup = heap_modify_tuple(oldpkgtup, tupDesc, values, nulls, replaces);
        simple_heap_update(pkgDesc, &tup->t_self, tup); 
        ReleaseSysCache(oldpkgtup);
        isReplaced = true; 
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("package spec not found")));
    }
    if (u_sess->attr.attr_common.IsInplaceUpgrade &&
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid != InvalidOid) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid);
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid = InvalidOid;
    }
    if (HeapTupleIsValid(tup)) {
        pkgOid = HeapTupleGetOid(tup);
    }
    Assert(OidIsValid(pkgOid));

    CatalogUpdateIndexes(pkgDesc, tup);

    if (isReplaced) {
        (void)deleteDependencyRecordsFor(PackageRelationId, pkgOid, true);
        /* the 'shared dependencies' also change when update. */
        deleteSharedDependencyRecordsFor(PackageRelationId, pkgOid, 0); 
        dropFunctionByPackageOid(pkgOid);
    }  

    heap_freetuple_ext(tup);

    heap_close(pkgDesc, RowExclusiveLock);

    /* Record dependencies */
    myself.classId = PackageRelationId;
    myself.objectId = pkgOid;
    myself.objectSubId = 0;

    if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId && !isReplaced)
        recordPinnedDependency(&myself);
    else {
        /* dependency on owner */
        referenced.classId = NamespaceRelationId;
        referenced.objectId = pkgNamespace;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        recordDependencyOnOwner(PackageRelationId, pkgOid, ownerId);

        recordDependencyOnCurrentExtension(&myself, false);
    }

    /* Post creation hook for new schema */
    InvokeObjectAccessHook(OAT_POST_CREATE, PackageRelationId, pkgOid, 0, NULL);

    /* Advance command counter so new tuple can be seen by validator */
    CommandCounterIncrement();

        /* Recode the procedure create time. */
    if (OidIsValid(pkgOid)) {
            if (!isReplaced) {
                PgObjectOption objectOpt = {true, true, false, false};
                CreatePgObject(pkgOid, OBJECT_TYPE_PKGSPEC, ownerId, objectOpt);
            } else {
                UpdatePgObjectMtime(pkgOid, OBJECT_TYPE_PROC);
            }
    }

    plpgsql_package_validator(pkgOid, false, true);

    return pkgOid;
}

bool IsFunctionInPackage(List* wholename) 
{
    char* objname = NULL;
    char* pkgname = NULL;
    char* schemaname = NULL;
    Oid pkgOid = InvalidOid;
    Oid namespaceId = InvalidOid;
    DeconstructQualifiedName(wholename, &schemaname, &objname, &pkgname);
    if (schemaname != NULL) {
        namespaceId = LookupNamespaceNoError(schemaname);
    }
    if (namespaceId != InvalidOid && pkgname != NULL) {
        pkgOid = PackageNameGetOid(pkgname, namespaceId);
    } else if(pkgname != NULL) {
        pkgOid = PackageNameGetOid(pkgname);
    } else {
        return false;
    }

    HeapTuple oldtup;
    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pkgOid));
    Relation pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(pg_proc_rel, InvalidOid, false, SnapshotNow, 1, &entry);
    while ((oldtup = systable_getnext(scan)) != NULL) {
        HeapTuple proctup = heap_copytuple(oldtup);
        Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
        if (!strcmp(objname, procform->proname.data)) {
            heap_freetuple(proctup);
            systable_endscan(scan);
            heap_close(pg_proc_rel, RowExclusiveLock);
            return true;
        }
        heap_freetuple(proctup);
    }    
    systable_endscan(scan);
    heap_close(pg_proc_rel, RowExclusiveLock);
    return false;
}
