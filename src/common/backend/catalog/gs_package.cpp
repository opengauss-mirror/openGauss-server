/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
#include "catalog/pg_synonym.h"
#include "commands/defrem.h"
#include "commands/sqladvisor.h"
#include "pgxc/pgxc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "tcop/autonomoustransaction.h"
#include "tcop/utility.h"
#include "utils/plpgsql.h"
#include "utils/pl_global_package_runtime_cache.h"
#include "utils/pl_package.h"

static PLpgSQL_datum* copypPackageVarDatum(PLpgSQL_datum* datum);
static PackageRuntimeState* buildPkgRunStatesbyPackage(PLpgSQL_package* pkg);
static PackageRuntimeState* buildPkgRunStatebyPkgRunState(PackageRuntimeState* parentPkgState);
static void copyCurrentSessionPkgs(SessionPackageRuntime* sessionPkgs, DList* pkgList);
static bool pkgExistInSession(PackageRuntimeState* pkgState);
static void copyParentSessionPkgs(SessionPackageRuntime* sessionPkgs, List* pkgList);
static void restorePkgValuesByPkgState(PLpgSQL_package* targetPkg, PackageRuntimeState* pkgState, bool isInit = false);
static void restoreAutonmSessionPkgs(SessionPackageRuntime* sessionPkgs);

static Acl* PackageAclDefault(Oid ownerId)
{
    AclMode world_default;
    AclMode owner_default;
    int nacl = 0;
    Acl* acl = NULL;
    AclItem* aip = NULL;
    world_default = ACL_NO_RIGHTS;
    owner_default = ACL_ALL_RIGHTS_PACKAGE;
    if (world_default != ACL_NO_RIGHTS)
        nacl++;
    if (owner_default != ACL_NO_RIGHTS)
        nacl++;
    acl = allocacl(nacl);
    aip = ACL_DAT(acl);
    if (world_default != ACL_NO_RIGHTS) {
        aip->ai_grantee = ACL_ID_PUBLIC;
        aip->ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(*aip, world_default, ACL_NO_RIGHTS);
        aip++;
    }

    if (owner_default != ACL_NO_RIGHTS) {
        aip->ai_grantee = ownerId;
        aip->ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(*aip, owner_default, ACL_NO_RIGHTS);
    }

    return acl;
}


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
        /* search for sysynonym object */
        pkgOid = SysynonymPkgNameGetOid(pkgname, namespaceId);
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
            pkgOid = SysynonymPkgNameGetOid(pkgname, namespaceId);
            if (OidIsValid(pkgOid)) {
                list_free_ext(tempActiveSearchPath);
                return pkgOid;
            }
        }

        list_free_ext(tempActiveSearchPath);
    }
    return InvalidOid;
}

Oid SysynonymPkgNameGetOid(const char* pkgname, Oid namespaceId)
{
    HeapTuple synTuple = NULL;
    Oid pkgOid = InvalidOid;
    synTuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(pkgname), ObjectIdGetDatum(namespaceId));
    if (HeapTupleIsValid(synTuple)) {
        Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(synTuple);
        namespaceId = get_namespace_oid(NameStr(synForm->synobjschema), true);
        pkgOid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(NameStr(synForm->synobjname)),
            ObjectIdGetDatum(namespaceId));
        ReleaseSysCache(synTuple);
    }
    return pkgOid;
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

Oid GetPackageNamespace(Oid packageOid) 
{
    Oid namespaceId = InvalidOid;
    HeapTuple pkgtup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
    if (!HeapTupleIsValid(pkgtup)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while compile package", packageOid),
                errdetail("cache lookup failed"),
                errcause("System error"),
                erraction("Drop and rebuild package")));
    }
    Form_gs_package packageform = (Form_gs_package)GETSTRUCT(pkgtup);
    namespaceId = packageform->pkgnamespace;
    ReleaseSysCache(pkgtup);
    return namespaceId;
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

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    pkg = plpgsql_package_validator(packageOid, isSpec);

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return pkg;
}

Oid PackageSpecCreate(Oid pkgNamespace, const char* pkgName, const Oid ownerId, const char* pkgSpecSrc, bool replace, bool pkgSecDef)
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
    bool isUpgrade = false;
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_common.plsql_show_all_error == true) {
        if (pkgSpecSrc == NULL) {
            return InvalidOid;
        } 
    }
#endif
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
    values[Anum_gs_package_pkgsecdef - 1] = BoolGetDatum(pkgSecDef);
    pkgacl = get_user_default_acl(ACL_OBJECT_PACKAGE, ownerId, pkgNamespace);
    if (pkgacl != NULL)
        values[Anum_gs_package_pkgacl - 1] = PointerGetDatum(pkgacl);
    else if (PLSQL_SECURITY_DEFINER) {
        values[Anum_gs_package_pkgacl - 1] = PointerGetDatum(PackageAclDefault(ownerId));
    } else {
        nulls[Anum_gs_package_pkgacl - 1] = true;
    }
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
        DeleteTypesDenpendOnPackage(PackageRelationId, pkgOid);
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
    isUpgrade = u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId && !isReplaced;
    if (isUpgrade) {
        recordPinnedDependency(&myself);
    } else {
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

    /* dependency between packages are only needed for package spec */
    if (!isUpgrade) {
        u_sess->plsql_cxt.pkg_dependencies = NIL;
        u_sess->plsql_cxt.need_pkg_dependencies = true;
    }
    PG_TRY();
    {
        plpgsql_package_validator(pkgOid, true, true);
    }
    PG_CATCH();
    {
        u_sess->plsql_cxt.need_pkg_dependencies = false;
        if (u_sess->plsql_cxt.pkg_dependencies != NIL) {
            list_free(u_sess->plsql_cxt.pkg_dependencies);
            u_sess->plsql_cxt.pkg_dependencies = NIL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->plsql_cxt.need_pkg_dependencies = false;

    /* record dependency discovered during validation */
    if (!isUpgrade && u_sess->plsql_cxt.pkg_dependencies != NIL) {
        recordDependencyOnPackage(PackageRelationId, pkgOid, u_sess->plsql_cxt.pkg_dependencies);
        list_free(u_sess->plsql_cxt.pkg_dependencies);
        u_sess->plsql_cxt.pkg_dependencies = NIL;
    }

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
    int i = 0;
    bool isReplaced = false;
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_common.plsql_show_all_error == true) {
        if (pkgBodySrc == NULL) {
            return InvalidOid;
        } 
    }
#endif
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
        DeleteTypesDenpendOnPackage(PackageRelationId, pkgOid, false);
    }

    heap_freetuple_ext(tup);

    heap_close(pkgDesc, NoLock);

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

static void free_var_value(PLpgSQL_var* var)
{
    if (var->freeval) {
        pfree(DatumGetPointer(var->value));
        var->freeval = false;
    }
}

void BuildSessionPackageRuntime(uint64 sessionId, uint64 parentSessionId)
{
    SessionPackageRuntime* parentSessionPkgs = NULL;

    /* get parent session pkgs, build current session pkgs need include them */
    if (parentSessionId != 0) {
        if (!u_sess->plsql_cxt.not_found_parent_session_pkgs) {
            if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL) {
                MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
                parentSessionPkgs = g_instance.global_session_pkg->fetch(parentSessionId);
                MemoryContextSwitchTo(oldcontext);
                u_sess->plsql_cxt.auto_parent_session_pkgs = parentSessionPkgs;
            } else {
                parentSessionPkgs = u_sess->plsql_cxt.auto_parent_session_pkgs;
            }
        }
    }
    MemoryContext pkgRuntimeCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SessionPackageRuntime",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(pkgRuntimeCtx);
    SessionPackageRuntime* resultSessionPkgs = (SessionPackageRuntime*)palloc0(sizeof(SessionPackageRuntime));
    resultSessionPkgs->context = pkgRuntimeCtx;
    if (u_sess->plsql_cxt.plpgsqlpkg_dlist_objects != NULL) {
        copyCurrentSessionPkgs(resultSessionPkgs, u_sess->plsql_cxt.plpgsqlpkg_dlist_objects);
    }

    if (parentSessionPkgs) {
        List* parentPkgList = parentSessionPkgs->runtimes;
        copyParentSessionPkgs(resultSessionPkgs, parentPkgList);
    } else if (parentSessionId != 0) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
    }

    g_instance.global_session_pkg->add(sessionId, resultSessionPkgs);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(resultSessionPkgs->context);
}

static void copyCurrentSessionPkgs(SessionPackageRuntime* sessionPkgs, DList* pkgList)
{
    PLpgSQL_package* pkg = NULL;
    DListCell* cell = NULL;
    dlist_foreach_cell(cell, pkgList) {
        pkg = ((plpgsql_pkg_HashEnt*)lfirst(cell))->package;
        PackageRuntimeState* pkgState = buildPkgRunStatesbyPackage(pkg);
        sessionPkgs->runtimes = lappend(sessionPkgs->runtimes, pkgState);
    }
}

static bool pkgExistInSession(PackageRuntimeState* pkgState)
{
    if (pkgState == NULL) {
        return false;
    }
    PLpgSQL_pkg_hashkey hashkey;
    hashkey.pkgOid = pkgState->packageId;
    PLpgSQL_package* getpkg = plpgsql_pkg_HashTableLookup(&hashkey);
    if (getpkg) {
        return true;
    } else {
        return false;
    }
}

static void copyParentSessionPkgs(SessionPackageRuntime* sessionPkgs, List* pkgList)
{
    ListCell* cell = NULL;
    foreach(cell, pkgList) {
        /* if package exist in current session, we already copy it */
        if (pkgExistInSession((PackageRuntimeState*)lfirst(cell))) {
            continue;
        }
        PackageRuntimeState* parentPkgState = (PackageRuntimeState*)lfirst(cell);
        PackageRuntimeState* pkgState = buildPkgRunStatebyPkgRunState(parentPkgState);
        sessionPkgs->runtimes = lappend(sessionPkgs->runtimes, pkgState);
    }
}

static PackageRuntimeState* buildPkgRunStatesbyPackage(PLpgSQL_package* pkg)
{
    PackageRuntimeState* pkgState = (PackageRuntimeState*)palloc0(sizeof(PackageRuntimeState));
    pkgState->packageId = pkg->pkg_oid;
    pkgState->size = pkg->ndatums;
    pkgState->datums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * pkg->ndatums);
    for (int i = 0; i < pkg->ndatums; i++) {
        pkgState->datums[i] = copypPackageVarDatum(pkg->datums[i]);
    }

    return pkgState;
}

static PackageRuntimeState* buildPkgRunStatebyPkgRunState(PackageRuntimeState* parentPkgState)
{
    PackageRuntimeState* pkgState = (PackageRuntimeState*)palloc0(sizeof(PackageRuntimeState));
    pkgState->packageId = parentPkgState->packageId;
    pkgState->size = parentPkgState->size;
    pkgState->datums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * parentPkgState->size);
    for (int i = 0; i < parentPkgState->size; i++) {
        pkgState->datums[i] = copypPackageVarDatum(parentPkgState->datums[i]);
    }

    return pkgState;
}

static PLpgSQL_datum* copypPackageVarDatum(PLpgSQL_datum* datum)
{
    PLpgSQL_datum* result = NULL;

    if (datum == NULL)
        return NULL;
    /* only VAR store value */
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* newm =  copyPlpgsqlVar((PLpgSQL_var*)datum);
            result = (PLpgSQL_datum*)newm;
            break;
        }
        default:
            break;
    }
    return result;
}

/* restore package values by Autonm SessionPkgs */
static void restoreAutonmSessionPkgs(SessionPackageRuntime* sessionPkgs)
{
    if (sessionPkgs == NULL || sessionPkgs->runtimes == NULL) {
        return;
    }

    ListCell* cell = NULL;
    PackageRuntimeState* pkgState = NULL;
    Oid pkgOid = InvalidOid;
    PLpgSQL_pkg_hashkey hashkey;
    PLpgSQL_package* pkg = NULL;

    foreach(cell, sessionPkgs->runtimes) {
        pkgState = (PackageRuntimeState*)lfirst(cell);
        pkgOid = pkgState->packageId;
        hashkey.pkgOid = pkgOid;
        pkg = plpgsql_pkg_HashTableLookup(&hashkey);
        if (pkg == NULL) {
            pkg = PackageInstantiation(pkgOid);
        }
        restorePkgValuesByPkgState(pkg, pkgState);
    }
}

/* restore package values by pkgState */
static void restorePkgValuesByPkgState(PLpgSQL_package* targetPkg, PackageRuntimeState* pkgState, bool isInit)
{
    if (targetPkg == NULL || pkgState == NULL) {
        return;
    }

    PLpgSQL_var* targetVar = NULL;
    PLpgSQL_var* fromVar = NULL;
    Datum newvalue = (Datum)0;

    /* this session pkg only contain spec, need compile body */
    if (!isInit && targetPkg->ndatums < pkgState->size) {
        targetPkg = PackageInstantiation(targetPkg->pkg_oid);
    }

    for (int i = 0; i < targetPkg->ndatums && i < pkgState->size; i++) {
        /* null mean datum not a var */
        if (pkgState->datums[i] == NULL) {
            continue;
        }

        fromVar = (PLpgSQL_var*)pkgState->datums[i];
        /* const value cannot be changed, cursor not supported by automo func yet */
        if (fromVar->isconst || fromVar->is_cursor_var || fromVar->datatype->typoid == REFCURSOROID) {
            continue;
        }

        newvalue = fromVar->value;
        targetVar = (PLpgSQL_var*)targetPkg->datums[i];
        bool isByReference = !targetVar->datatype->typbyval && !fromVar->isnull;
        if (isByReference) {
            MemoryContext temp = MemoryContextSwitchTo(targetVar->pkg->pkg_cxt);
            newvalue = datumCopy(fromVar->value, false, targetVar->datatype->typlen);
            MemoryContextSwitchTo(temp);
        }
        free_var_value(targetVar);

        targetVar->value = newvalue;
        targetVar->isnull = fromVar->isnull;
        if (isByReference) {
            targetVar->freeval = true;
        }

        if (fromVar->tableOfIndex != NULL) {
            MemoryContext temp = MemoryContextSwitchTo(targetVar->pkg->pkg_cxt);
            targetVar->tableOfIndex = copyTableOfIndex(fromVar->tableOfIndex);
            MemoryContextSwitchTo(temp);
        } else if (fromVar->isnull) {
            hash_destroy(targetVar->tableOfIndex);
            targetVar->tableOfIndex = NULL;
        }
    }
}

/* init Autonomous session package values by parent session */
void initAutonomousPkgValue(PLpgSQL_package* targetPkg, uint64 sessionId)
{
    if (u_sess->plsql_cxt.not_found_parent_session_pkgs) {
        return;
    }

    SessionPackageRuntime* sessionPkgs = NULL;
    if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        sessionPkgs = g_instance.global_session_pkg->fetch(sessionId);
        MemoryContextSwitchTo(oldcontext);
        u_sess->plsql_cxt.auto_parent_session_pkgs = sessionPkgs;
    } else {
        sessionPkgs = u_sess->plsql_cxt.auto_parent_session_pkgs;
    }

    if (sessionPkgs == NULL) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
        return;
    }

    ListCell* cell = NULL;
    PackageRuntimeState* pkgState = NULL;

    foreach(cell, sessionPkgs->runtimes) {
        pkgState = (PackageRuntimeState*)lfirst(cell);
        if (targetPkg->pkg_oid == pkgState->packageId) {
            restorePkgValuesByPkgState(targetPkg, pkgState, true);
            break;
        } else {
            continue;
        }
    }
}

void processAutonmSessionPkgs(PLpgSQL_function* func)
{
    /* ignore inline_code_block function */
    if (!OidIsValid(func->fn_oid)) {
        return;
    }

    uint64 currentSessionId = IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;

    if (IsAutonomousTransaction(func->action->isAutonomous)) {
        /*
         * call after plpgsql_exec_autonm_function(), need restore
         * autonm session pkgs to current session, and remove
         * sessionpkgs from g_instance.global_session_pkg.
         */
        uint64 automnSessionId = u_sess->SPI_cxt.autonomous_session->current_attach_sessionid;
        SessionPackageRuntime* sessionpkgs = g_instance.global_session_pkg->fetch(automnSessionId);
        restoreAutonmSessionPkgs(sessionpkgs);
        if (sessionpkgs != NULL) {
            MemoryContextDelete(sessionpkgs->context);
            g_instance.global_session_pkg->remove(automnSessionId);
        }
        g_instance.global_session_pkg->remove(currentSessionId);
    } else {
        /*
         * call after plpgsql_exec_function(), If it is first level
         * autonomous func, need add its all session package values
         * to global, the parent session will fetch the sessionPkgs,
         * and restore package values by it.
         */
        if (u_sess->is_autonomous_session == true && u_sess->SPI_cxt._connected == 0) {
            BuildSessionPackageRuntime(currentSessionId, 0);
        }
    }
}
