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
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_synonym.h"
#include "commands/defrem.h"
#include "commands/sqladvisor.h"
#include "gs_thread.h"
#include "parser/parse_type.h"
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

#include "tcop/pquery.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"

static PLpgSQL_datum* CopyPackageVarDatum(PLpgSQL_datum* datum);
static PackageRuntimeState* BuildPkgRunStatesbyPackage(PLpgSQL_package* pkg);
static PackageRuntimeState* BuildPkgRunStatebyPkgRunState(PackageRuntimeState* parentPkgState);
static void CopyCurrentSessionPkgs(SessionPackageRuntime* sessionPkgs, DList* pkgList);
static bool PkgExistInSession(PackageRuntimeState* pkgState);
static void CopyParentSessionPkgs(SessionPackageRuntime* sessionPkgs, List* pkgList);
static void RestorePkgValuesByPkgState(PLpgSQL_package* targetPkg, PackageRuntimeState* pkgState, bool isInit = false);
static void RestoreAutonmSessionPkgs(SessionPackageRuntime* sessionPkgs);
static void ReleaseUnusedPortalContext(List* portalContexts, bool releaseAll = false);
#define MAXSTRLEN ((1 << 11) - 1)

static Acl* PackageAclDefault(Oid ownerId)
{
    AclMode owner_default;
    int nacl = 0;
    Acl* acl = NULL;
    AclItem* aip = NULL;
    owner_default = ACL_ALL_RIGHTS_PACKAGE;
    if (owner_default != ACL_NO_RIGHTS)
        nacl++;
    acl = allocacl(nacl);
    aip = ACL_DAT(acl);

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
    else if (PLSQL_SECURITY_DEFINER && u_sess->attr.attr_common.upgrade_mode == 0) {
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
                (errcode(ERRCODE_DUPLICATE_PACKAGE),
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
            if (!pg_package_ownercheck(HeapTupleGetOid(oldpkgtup), ownerId)) {
                ReleaseSysCache(oldpkgtup);
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PACKAGE, pkgName);
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
        DeleteFunctionByPackageOid(pkgOid);
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
    oldPkgOid = PackageNameGetOid(pkgName, pkgNamespace);
    if (!OidIsValid(oldPkgOid)) {
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
        } else if (!isNull) {
            DeleteFunctionByPackageOid(oldPkgOid);
            DeleteTypesDenpendOnPackage(PackageRelationId, oldPkgOid, false);
        }
        tup = heap_modify_tuple(oldpkgtup, tupDesc, values, nulls, replaces);
        simple_heap_update(pkgDesc, &tup->t_self, tup); 
        ReleaseSysCache(oldpkgtup);
        isReplaced = true; 
    }
    if (u_sess->attr.attr_common.IsInplaceUpgrade &&
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid != InvalidOid) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid);
        u_sess->upg_cxt.Inplace_upgrade_next_gs_package_oid = InvalidOid;
    }
    if (HeapTupleIsValid(tup)) {
        oldPkgOid = HeapTupleGetOid(tup);
    }

    CatalogUpdateIndexes(pkgDesc, tup);

    heap_freetuple_ext(tup);

    heap_close(pkgDesc, RowExclusiveLock);

    /* Post creation hook for new schema */
    InvokeObjectAccessHook(OAT_POST_CREATE, PackageRelationId, oldPkgOid, 0, NULL);

    /* Advance command counter so new tuple can be seen by validator */
    CommandCounterIncrement();

        /* Recode the procedure create time. */
    if (OidIsValid(oldPkgOid)) {
        if (!isReplaced) {
            PgObjectOption objectOpt = {true, true, false, false};
            CreatePgObject(oldPkgOid, OBJECT_TYPE_PKGSPEC, ownerId, objectOpt);
        } else {
            UpdatePgObjectMtime(oldPkgOid, OBJECT_TYPE_PROC);
        }
    }

    plpgsql_package_validator(oldPkgOid, false, true);

    return oldPkgOid;
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

/* free the reference value of var */
static void free_var_value(PLpgSQL_var* var)
{
    if (var->freeval) {
        /* means the value is by reference, and need free */
        pfree(DatumGetPointer(var->value));
        var->freeval = false;
    }
}

/* find auto session stored portals, and build new on depend on it */
Portal BuildHoldPortalFromAutoSession(PLpgSQL_execstate* estate, int curVarDno, int outParamIndex)
{
    AutoSessionPortalData* holdPortal = NULL;
    ListCell* cell = NULL;
    PLpgSQL_var *curVar = NULL;
    foreach(cell, u_sess->plsql_cxt.storedPortals) {
        AutoSessionPortalData* portalData = (AutoSessionPortalData*)lfirst(cell);
        if (portalData != NULL && portalData->outParamIndex == outParamIndex) {
            holdPortal = portalData;
        }
    }

    if (holdPortal == NULL) {
        /* reset the cursor attribute */
        curVar = (PLpgSQL_var*)(estate->datums[curVarDno]);
        free_var_value(curVar);
        curVar->value = (Datum)0;
        curVar->isnull = true;
        curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ISOPEN]);
        curVar->value = BoolGetDatum(false);
        curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_FOUND]);
        curVar->value = BoolGetDatum(false);
        curVar->isnull = true;
        curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_NOTFOUND]);
        curVar->value = BoolGetDatum(false);
        curVar->isnull = true;
        curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ROWCOUNT]);
        curVar->value = Int32GetDatum(0);
        curVar->isnull = true;
        return NULL;
    }
    /* Reset SPI result (note we deliberately don't touch lastoid) */
    SPI_processed = 0;
    SPI_tuptable = NULL;
    u_sess->SPI_cxt._current->processed = 0;
    u_sess->SPI_cxt._current->tuptable = NULL;

    SPI_STACK_LOG("begin", NULL, NULL);
    if (_SPI_begin_call(true) < 0) {
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI stack is corrupted when perform cursor operation, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    Portal portal = CreateNewPortal(true);
    portal->holdContext = holdPortal->holdContext;
    portal->holdStore = holdPortal->holdStore;
    portal->tupDesc = holdPortal->tupDesc;
    portal->strategy = holdPortal->strategy;
    portal->cursorOptions = holdPortal->cursorOptions;
    portal->commandTag = holdPortal->commandTag;
    portal->atEnd = holdPortal->atEnd;
    portal->atStart = holdPortal->atStart;
    portal->portalPos = holdPortal->portalPos;

    portal->autoHeld = true;
    portal->resowner = NULL;
    portal->createSubid = InvalidSubTransactionId;
    portal->activeSubid = InvalidSubTransactionId;
    portal->status = PORTAL_READY;

    portal->portalPinned = true;

    /* Pop the SPI stack */
    SPI_STACK_LOG("end", NULL, NULL);
    _SPI_end_call(true);

    /* restore cursor var values */
    curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ISOPEN]);
    curVar->value = BoolGetDatum(holdPortal->is_open);
    curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_FOUND]);
    curVar->value = BoolGetDatum(holdPortal->found);
    curVar->isnull = holdPortal->null_fetch;
    curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_NOTFOUND]);
    curVar->value = BoolGetDatum(holdPortal->not_found);
    curVar->isnull = holdPortal->null_fetch;
    curVar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ROWCOUNT]);
    curVar->value = Int32GetDatum(holdPortal->row_count);
    curVar->isnull = holdPortal->null_open;

    return portal;
}

static void ReleaseUnusedPortalContext(List* portalContexts, bool releaseAll)
{
    ListCell* cell = NULL;
    AutoSessionPortalContextData* portalContext = NULL;
    foreach(cell, portalContexts) {
        portalContext = (AutoSessionPortalContextData*)lfirst(cell);
        if (releaseAll || portalContext->status == CONTEXT_NEW) {
            /*
             * if context not new, its session id and parent is from auto session.
             * we should set them to parent session to delete it.
             */
            if (portalContext->status != CONTEXT_NEW) {
                portalContext->portalHoldContext->session_id = u_sess->session_id;
                portalContext->portalHoldContext->thread_id = gs_thread_self();
                MemoryContextSetParent(portalContext->portalHoldContext, u_sess->top_portal_cxt);
            }
            MemoryContextDelete(portalContext->portalHoldContext);
        }
    }
}

/* restore cursors from auto transaction procedure out param */
void restoreAutonmSessionCursors(PLpgSQL_execstate* estate, PLpgSQL_row* row)
{
    if (!u_sess->plsql_cxt.call_after_auto) {
            return;
    }
    PLpgSQL_var* curvar = NULL;
    for (int i = 0; i < row->nfields; i++) {
        if (estate->datums[row->varnos[i]]->dtype != PLPGSQL_DTYPE_VAR) {
            continue;
        }
        curvar = (PLpgSQL_var*)(estate->datums[row->varnos[i]]);
        if (curvar->datatype->typoid == REFCURSOROID) {
            Portal portal = BuildHoldPortalFromAutoSession(estate, row->varnos[i], i);
            if (portal == NULL) {
                continue;
            }
            if (curvar->pkg != NULL) {
                MemoryContext temp = MemoryContextSwitchTo(curvar->pkg->pkg_cxt);
                assign_text_var(curvar, portal->name);
                temp = MemoryContextSwitchTo(temp);
            } else {
                assign_text_var(curvar, portal->name);
            }
            curvar->cursor_closed = false;
        }
    }

    list_free_deep(u_sess->plsql_cxt.storedPortals);
    u_sess->plsql_cxt.storedPortals = NIL;
    ReleaseUnusedPortalContext(u_sess->plsql_cxt.portalContext);
    list_free_deep(u_sess->plsql_cxt.portalContext);
    u_sess->plsql_cxt.portalContext = NIL;
    u_sess->plsql_cxt.call_after_auto = false;
}

static bool PortalConextInList(Portal portal, List* PortalContextList)
{
    ListCell* cell = NULL;
    AutoSessionPortalContextData* portalContext = NULL;
    foreach(cell, PortalContextList) {
        portalContext = (AutoSessionPortalContextData*)lfirst(cell);
        if (portalContext != NULL && portalContext->portalHoldContext == portal->holdContext) {
            return true;
        }
    }
    return false;
}

void ResetAutoPortalConext(Portal portal)
{
    if (portal->holdStore == NULL) {
        return;
    }
    /*
     * we do not call tuplestore_end, because context is another session,
     * this may cause memory leak, but it seems not serious, because autonomous
     * session will destroy soon.
     */
    portal->holdStore = NULL;
    List* PortalContextList = u_sess->plsql_cxt.auto_parent_session_pkgs->portalContext;
    ListCell* cell = NULL;
    AutoSessionPortalContextData* portalContext = NULL;
    /* mark context is new, and can be re-used */
    foreach(cell, PortalContextList) {
        portalContext = (AutoSessionPortalContextData*)lfirst(cell);
        if (portalContext != NULL && portalContext->portalHoldContext == portal->holdContext) {
            portalContext->status = CONTEXT_NEW;
            if (u_sess->plsql_cxt.parent_context != NULL) {
                portalContext->portalHoldContext->session_id = u_sess->plsql_cxt.parent_session_id;
                portalContext->portalHoldContext->thread_id = u_sess->plsql_cxt.parent_thread_id;
                MemoryContextSetParent(portalContext->portalHoldContext, u_sess->plsql_cxt.parent_context);
            }
        }
    }
}

MemoryContext GetAvailableHoldContext(List* PortalContextList)
{
    ListCell* cell = NULL;
    AutoSessionPortalContextData* portalContext = NULL;
    MemoryContext result = NULL;
    foreach(cell, PortalContextList) {
        portalContext = (AutoSessionPortalContextData*)lfirst(cell);
        if (portalContext != NULL && portalContext->status == CONTEXT_NEW) {
            result = portalContext->portalHoldContext;
            portalContext->status = CONTEXT_USED;
            break;
        }
    }

    if (result == NULL) {
        ereport(ERROR,
            (errmodule(MOD_GPRC), errcode(ERRCODE_INVALID_STATUS),
                errmsg("no available portal hold context"),
                errdetail("no available portal hold context when hold autonomous transction procedure cursor"),
                errcause("System error"),
                erraction("Modify autonomous transction procedure")));
    }


    u_sess->plsql_cxt.parent_session_id = result->session_id;
    u_sess->plsql_cxt.parent_thread_id = result->thread_id;
    u_sess->plsql_cxt.parent_context = result->parent;
    result->session_id = u_sess->session_id;
    result->thread_id = gs_thread_self();
    MemoryContextSetParent(result, u_sess->top_portal_cxt);

    return result;
}

static List* BuildPortalContextListForAutoSession(PLpgSQL_function* func)
{
    List* result = NIL;
    if (func == NULL) {
        return result;
    }

    if (func->out_param_varno == -1) {
        return result;
    }

    AutoSessionPortalContextData* portalContext = NULL;
    PLpgSQL_datum* outDatum = func->datums[func->out_param_varno];
    if (outDatum->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* outVar = (PLpgSQL_var*)outDatum;
        if (outVar == NULL || outVar->datatype == NULL || outVar->datatype->typoid != REFCURSOROID) {
            return result;
        }
        portalContext = (AutoSessionPortalContextData*)palloc0(sizeof(AutoSessionPortalContextData));
        portalContext->status = CONTEXT_NEW;
        portalContext->portalHoldContext = AllocSetContextCreate(u_sess->top_portal_cxt,
            "PortalHoldContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        result = lappend(result, portalContext);
        return result;
    }

    PLpgSQL_row* outRow = (PLpgSQL_row*)outDatum;
    if (outRow->refname != NULL) {
        /* means out param is just one normal row variable */
        return result;
    }
    for (int i = 0; i < outRow->nfields; i++) {
        if (func->datums[outRow->varnos[i]]->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)(func->datums[outRow->varnos[i]]);
            if (var != NULL && var->datatype != NULL && var->datatype->typoid == REFCURSOROID) {
                portalContext = (AutoSessionPortalContextData*)palloc0(sizeof(AutoSessionPortalContextData));
                portalContext->status = CONTEXT_NEW;
                portalContext->portalHoldContext = AllocSetContextCreate(u_sess->top_portal_cxt,
                    "PortalHoldContext",
                    ALLOCSET_DEFAULT_MINSIZE,
                    ALLOCSET_DEFAULT_INITSIZE,
                    ALLOCSET_DEFAULT_MAXSIZE);
                result = lappend(result, portalContext);
            }
        }
    }
    return result;
}

static void HoldOutParamPortal(Portal portal)
{
    if (portal->portalPinned && !portal->autoHeld) {
        /*
         * Doing transaction control, especially abort, inside a cursor
         * loop that is not read-only, for example using UPDATE
         * ... RETURNING, has weird semantics issues.  Also, this
         * implementation wouldn't work, because such portals cannot be
         * held.  (The core grammar enforces that only SELECT statements
         * can drive a cursor, but for example PL/pgSQL does not restrict
         * it.)
         */
        if (portal->strategy != PORTAL_ONE_SELECT) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
                errmsg("cannot perform transaction commands inside a cursor loop that is not read-only")));
        }

        /* skip portal got error */
        if (portal->status == PORTAL_FAILED)
            return;

        /* Verify it's in a suitable state to be held */
        if (portal->status != PORTAL_READY)
            ereport(ERROR, (errmsg("pinned portal(%s) is not ready to be auto-held, with status[%d]",
                portal->name, portal->status)));

        HoldPortal(portal);
        portal->autoHeld = true;
    }
}

static AutoSessionPortalData* BuildPortalDataForParentSession(PLpgSQL_execstate* estate,
    int curVarDno, int outParamIndex)
{
    PLpgSQL_var* curvar = (PLpgSQL_var*)(estate->datums[curVarDno]);
    if (curvar->isnull) {
        return NULL;
    }
    char* curname = TextDatumGetCString(curvar->value);
    Portal portal = SPI_cursor_find(curname);
    pfree_ext(curname);
    if (portal == NULL) {
        return NULL;
    }

    /* portal not held, hold it */
    if (portal->portalPinned && !portal->autoHeld) {
        /* Push the SPI stack */
        SPI_STACK_LOG("begin", NULL, NULL);
        if (_SPI_begin_call(true) < 0) {
            ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI stack is corrupted when perform cursor operation, current level: %d, connected level: %d",
                u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
        }
        HoldOutParamPortal(portal);
        /* Pop the SPI stack */
        SPI_STACK_LOG("end", NULL, NULL);
        _SPI_end_call(true);
    }

    if (!portal->autoHeld) {
        return NULL;
    }

    if (!PortalConextInList(portal, u_sess->plsql_cxt.auto_parent_session_pkgs->portalContext)) {
        ereport(ERROR,
            (errmodule(MOD_GPRC),
             errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("nested call of ref cursor out param for autonomous transaction procedure is not supported yet."),
             errdetail("N/A"),
             errcause("feature not supported"),
             erraction("Modify autonomous transction procedure")));
    }

    MemoryContext oldCtx = MemoryContextSwitchTo(portal->holdContext);
    AutoSessionPortalData* portalData = (AutoSessionPortalData*)palloc0(sizeof(AutoSessionPortalData));
    portalData->outParamIndex = outParamIndex;
    portalData->strategy = portal->strategy;
    portalData->cursorOptions = portal->cursorOptions;
    portalData->commandTag = pstrdup(portal->commandTag);
    portalData->atEnd = portal->atEnd;
    portalData->atStart = portal->atStart;
    portalData->portalPos = portal->portalPos;
    portalData->holdStore = portal->holdStore;
    portalData->holdContext = portal->holdContext;
    portalData->tupDesc = CreateTupleDescCopy(portal->tupDesc);
    /* cursor attribute data */
    curvar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ISOPEN]);
    portalData->is_open = DatumGetBool(curvar->value);
    portalData->null_open = curvar->isnull;
    curvar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_FOUND]);
    portalData->found = DatumGetBool(curvar->value);
    portalData->null_fetch = curvar->isnull;
    curvar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_NOTFOUND]);
    portalData->not_found = DatumGetBool(curvar->value);
    curvar = (PLpgSQL_var*)(estate->datums[curVarDno + CURSOR_ROWCOUNT]);
    portalData->row_count = DatumGetInt32(curvar->value);
    MemoryContextSwitchTo(oldCtx);
    if (u_sess->plsql_cxt.parent_context != NULL) {
        portalData->holdContext->session_id = u_sess->plsql_cxt.parent_session_id;
        portalData->holdContext->thread_id = u_sess->plsql_cxt.parent_thread_id;
        MemoryContextSetParent(portalData->holdContext, u_sess->plsql_cxt.parent_context);
    }

    return portalData;
}

static List* BuildPortalDataListForParentSession(PLpgSQL_execstate* estate)
{
    List* result = NIL;
    if (estate == NULL) {
        return result;
    }
    SessionPackageRuntime* sessionPkgs = NULL;
    if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL) {
        return result;
    } else {
        sessionPkgs = u_sess->plsql_cxt.auto_parent_session_pkgs;
    }

    /* means no out param */
    if (estate->func->out_param_varno == -1) {
        return result;
    }

    PLpgSQL_datum* outDatum = estate->datums[estate->func->out_param_varno];
    AutoSessionPortalData* portalData = NULL;
    if (outDatum->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* outVar = (PLpgSQL_var*)outDatum;
        if (outVar == NULL || outVar->datatype == NULL || outVar->datatype->typoid != REFCURSOROID) {
            return result;
        }
        portalData = BuildPortalDataForParentSession(estate, estate->func->out_param_varno, 0);
        if (portalData != NULL) {
            result = lappend(result, portalData);
        }
        return result;
    }

    PLpgSQL_row* outRow = (PLpgSQL_row*)outDatum;
    if (outRow->refname != NULL) {
        /* means out param is just one normal row variable */
        return result;
    }
    for (int i = 0; i < outRow->nfields; i++) {
        if (estate->datums[outRow->varnos[i]]->dtype != PLPGSQL_DTYPE_VAR) {
            continue;
        }
        PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[outRow->varnos[i]]);
        if (var != NULL && var->datatype != NULL && var->datatype->typoid == REFCURSOROID) {
            portalData = BuildPortalDataForParentSession(estate, outRow->varnos[i], i);
            if (portalData != NULL) {
                result = lappend(result, portalData);
            }
        }
    }

    u_sess->plsql_cxt.parent_session_id = 0;
    u_sess->plsql_cxt.parent_thread_id = 0;
    u_sess->plsql_cxt.parent_context = NULL;
    return result;
}

static List* BuildFuncInfoList(PLpgSQL_execstate* estate)
{
    List* result = NIL;
    if (estate == NULL) {
        return result;
    }

    AutoSessionFuncValInfo *autoSessionFuncInfo = (AutoSessionFuncValInfo *)palloc0(sizeof(AutoSessionFuncValInfo));
    if (COMPAT_CURSOR) {
        PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[estate->found_varno]);
        autoSessionFuncInfo->found = var->value;
        var = (PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]);
        if (var->isnull) {
            autoSessionFuncInfo->sql_cursor_found = PLPGSQL_NULL;
        } else {
            autoSessionFuncInfo->sql_cursor_found = var->value;
        }
        var = (PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]);
        if (var->isnull) {
            autoSessionFuncInfo->sql_notfound = PLPGSQL_NULL;
        } else {
            autoSessionFuncInfo->sql_notfound = var->value;
        }

        var = (PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]);
        autoSessionFuncInfo->sql_isopen = var->value;
        var = (PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]);
        autoSessionFuncInfo->sql_rowcount = var->value;
    }
    PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[estate->sqlcode_varno]);
    if (var->isnull) {
        autoSessionFuncInfo->sqlcode_isnull = true;
        autoSessionFuncInfo->sqlcode = 0;
    } else {
        autoSessionFuncInfo->sqlcode_isnull = false;
        char* sqlcode = TextDatumGetCString(var->value);
        autoSessionFuncInfo->sqlcode = MAKE_SQLSTATE((unsigned char)sqlcode[0], (unsigned char)sqlcode[1],
                                                     (unsigned char)sqlcode[2], (unsigned char)sqlcode[3],
                                                     (unsigned char)sqlcode[4]);
    }
    result = lappend(result, autoSessionFuncInfo);

    return result;
}


void BuildSessionPackageRuntimeForAutoSession(uint64 sessionId, uint64 parentSessionId,
    PLpgSQL_execstate* estate, PLpgSQL_function* func)
{
    MemoryContext pkgRuntimeCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SessionPackageRuntime",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(pkgRuntimeCtx);
    SessionPackageRuntime* resultSessionPkgs = (SessionPackageRuntime*)palloc0(sizeof(SessionPackageRuntime));
    resultSessionPkgs->context = pkgRuntimeCtx;

    /* doing insert gs_source, no need to restore package value */
    if (func->is_insert_gs_source) {
        resultSessionPkgs->is_insert_gs_source = true;
        g_instance.global_session_pkg->Add(sessionId, resultSessionPkgs);
        MemoryContextSwitchTo(oldCtx);
        MemoryContextDelete(resultSessionPkgs->context);
        return;
    }

    if (u_sess->plsql_cxt.plpgsqlpkg_dlist_objects != NULL) {
        CopyCurrentSessionPkgs(resultSessionPkgs, u_sess->plsql_cxt.plpgsqlpkg_dlist_objects);
    }

    SessionPackageRuntime* parentSessionPkgs = NULL;
    /* get parent session pkgs, build current session pkgs need include them */
    if (parentSessionId != 0) {
        if (!u_sess->plsql_cxt.not_found_parent_session_pkgs) {
            if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL) {
                MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
                parentSessionPkgs = g_instance.global_session_pkg->Fetch(parentSessionId);
                MemoryContextSwitchTo(oldcontext);
                u_sess->plsql_cxt.auto_parent_session_pkgs = parentSessionPkgs;
            } else {
                parentSessionPkgs = u_sess->plsql_cxt.auto_parent_session_pkgs;
            }
        }
    }

    if (parentSessionPkgs) {
        List* parentPkgList = parentSessionPkgs->runtimes;
        CopyParentSessionPkgs(resultSessionPkgs, parentPkgList);
    } else if (parentSessionId != 0) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
    }

    /* build portal context for auto session */
    resultSessionPkgs->portalContext = BuildPortalContextListForAutoSession(func);

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        resultSessionPkgs->funcValInfo = BuildFuncInfoList(estate);
    }

    g_instance.global_session_pkg->Add(sessionId, resultSessionPkgs);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(resultSessionPkgs->context);
}

void BuildSessionPackageRuntimeForParentSession(uint64 sessionId, PLpgSQL_execstate* estate)
{
    MemoryContext pkgRuntimeCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SessionPackageRuntime",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(pkgRuntimeCtx);
    SessionPackageRuntime* resultSessionPkgs = (SessionPackageRuntime*)palloc0(sizeof(SessionPackageRuntime));
    resultSessionPkgs->context = pkgRuntimeCtx;
    if (u_sess->plsql_cxt.plpgsqlpkg_dlist_objects != NULL) {
        CopyCurrentSessionPkgs(resultSessionPkgs, u_sess->plsql_cxt.plpgsqlpkg_dlist_objects);
    }

    /* build portal data for out param ref cursor of automous transaction */
    resultSessionPkgs->portalData = BuildPortalDataListForParentSession(estate);

    /* copy the portal context return to parent session */
    if (u_sess->plsql_cxt.auto_parent_session_pkgs != NULL) {
        resultSessionPkgs->portalContext =
            CopyPortalContexts(u_sess->plsql_cxt.auto_parent_session_pkgs->portalContext);
    }

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        resultSessionPkgs->funcValInfo = BuildFuncInfoList(estate);
    }

    g_instance.global_session_pkg->Add(sessionId, resultSessionPkgs);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(resultSessionPkgs->context);
}

static void CopyCurrentSessionPkgs(SessionPackageRuntime* sessionPkgs, DList* pkgList)
{
    PLpgSQL_package* pkg = NULL;
    DListCell* cell = NULL;
    dlist_foreach_cell(cell, pkgList) {
        pkg = ((plpgsql_pkg_HashEnt*)lfirst(cell))->package;
        PackageRuntimeState* pkgState = BuildPkgRunStatesbyPackage(pkg);
        sessionPkgs->runtimes = lappend(sessionPkgs->runtimes, pkgState);
    }
}

static bool PkgExistInSession(PackageRuntimeState* pkgState)
{
    if (pkgState == NULL) {
        return false;
    }
    PLpgSQL_pkg_hashkey hashkey;
    hashkey.pkgOid = pkgState->packageId;
    PLpgSQL_package* getpkg = plpgsql_pkg_HashTableLookup(&hashkey);
    return getpkg ? true : false;
}

static void CopyParentSessionPkgs(SessionPackageRuntime* sessionPkgs, List* pkgList)
{
    ListCell* cell = NULL;
    foreach(cell, pkgList) {
        /* if package exist in current session, we already copy it */
        if (PkgExistInSession((PackageRuntimeState*)lfirst(cell))) {
            continue;
        }
        PackageRuntimeState* parentPkgState = (PackageRuntimeState*)lfirst(cell);
        PackageRuntimeState* pkgState = BuildPkgRunStatebyPkgRunState(parentPkgState);
        sessionPkgs->runtimes = lappend(sessionPkgs->runtimes, pkgState);
    }
}

static PackageRuntimeState* BuildPkgRunStatesbyPackage(PLpgSQL_package* pkg)
{
    PackageRuntimeState* pkgState = (PackageRuntimeState*)palloc0(sizeof(PackageRuntimeState));
    pkgState->packageId = pkg->pkg_oid;
    pkgState->size = pkg->ndatums;
    pkgState->datums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * pkg->ndatums);
    for (int i = 0; i < pkg->ndatums; i++) {
        pkgState->datums[i] = CopyPackageVarDatum(pkg->datums[i]);
    }

    return pkgState;
}

static PackageRuntimeState* BuildPkgRunStatebyPkgRunState(PackageRuntimeState* parentPkgState)
{
    PackageRuntimeState* pkgState = (PackageRuntimeState*)palloc0(sizeof(PackageRuntimeState));
    pkgState->packageId = parentPkgState->packageId;
    pkgState->size = parentPkgState->size;
    pkgState->datums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * parentPkgState->size);
    for (int i = 0; i < parentPkgState->size; i++) {
        pkgState->datums[i] = CopyPackageVarDatum(parentPkgState->datums[i]);
    }

    return pkgState;
}

static PLpgSQL_datum* CopyPackageVarDatum(PLpgSQL_datum* datum)
{
    PLpgSQL_datum* result = NULL;

    if (datum == NULL)
        return NULL;
    /* only VAR store value */
    if (datum->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* var = (PLpgSQL_var*)datum;
        if (unlikely(var->nest_table != NULL))
            return NULL;
        PLpgSQL_var* newm =  copyPlpgsqlVar(var);
        result = (PLpgSQL_datum*)newm;
    }
    return result;
}

/* restore package values by Autonm SessionPkgs */
static void RestoreAutonmSessionPkgs(SessionPackageRuntime* sessionPkgs)
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
        RestorePkgValuesByPkgState(pkg, pkgState);
    }
}

/* restore package values by pkgState */
static void RestorePkgValuesByPkgState(PLpgSQL_package* targetPkg, PackageRuntimeState* pkgState, bool isInit)
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

    int startNum = 0;
    int endNum = targetPkg->ndatums < pkgState->size ? targetPkg->ndatums : pkgState->size;
    /* when compiling body, need not restore public var */
    if (isInit && targetPkg->is_bodycompiled) {
        startNum = targetPkg->public_ndatums;
    }

    for (int i = startNum; i < endNum; i++) {
        fromVar = (PLpgSQL_var*)pkgState->datums[i];
        /* null mean datum not a var, no need to restore */
        if (fromVar == NULL) {
            continue;
        }
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

static SessionPackageRuntime* GetSessPkgRuntime(uint64 sessionId)
{
    SessionPackageRuntime* sessionPkgs = NULL;
    if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        sessionPkgs = g_instance.global_session_pkg->Fetch(sessionId);
        MemoryContextSwitchTo(oldcontext);
        u_sess->plsql_cxt.auto_parent_session_pkgs = sessionPkgs;
    } else {
        sessionPkgs = u_sess->plsql_cxt.auto_parent_session_pkgs;
    }
    return sessionPkgs;
}


/* update packages already in auto session by parent session */
void initAutoSessionPkgsValue(uint64 sessionId)
{
    if (u_sess->is_autonomous_session != true || u_sess->SPI_cxt._connected != 0) {
        return;
    }

    ListCell* cell = NULL;
    PackageRuntimeState* pkgState = NULL;
    Oid pkgOid = InvalidOid;
    PLpgSQL_pkg_hashkey hashkey;
    PLpgSQL_package* pkg = NULL;

    SessionPackageRuntime* sessionPkgs = GetSessPkgRuntime(sessionId);

    if (sessionPkgs == NULL) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
        return;
    }
    if (sessionPkgs->runtimes == NULL) {
        return;
    }
    if (sessionPkgs->is_insert_gs_source) {
        return;
    }

    foreach(cell, sessionPkgs->runtimes) {
        pkgState = (PackageRuntimeState*)lfirst(cell);
        pkgOid = pkgState->packageId;
        hashkey.pkgOid = pkgOid;
        pkg = plpgsql_pkg_HashTableLookup(&hashkey);
        if (pkg == NULL) {
            continue;
        }
        RestorePkgValuesByPkgState(pkg, pkgState);
    }
}

void setCursorAtrValue(PLpgSQL_execstate* estate, AutoSessionFuncValInfo* FuncValInfo)
{
    PLpgSQL_var* var = NULL;
    
    var = (PLpgSQL_var*)(estate->datums[estate->found_varno]);
    var->value = BoolGetDatum(FuncValInfo->found);
    var->isnull = false;
    
    /* Set the magic implicit cursor attribute variable FOUND to false */
    var = (PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]);
    /* if state is -1, found is set NULL */
    if (FuncValInfo->sql_cursor_found == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (FuncValInfo->sql_cursor_found == 1) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
    
    /* Set the magic implicit cursor attribute variable NOTFOUND to true */
    var = (PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]);
    /* if state is -1, notfound is set NULL */
    if (FuncValInfo->sql_notfound == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (FuncValInfo->sql_notfound == 1) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
    
    /* Set the magic implicit cursor attribute variable ISOPEN to false */
    var = (PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]);
    var->value = BoolGetDatum(FuncValInfo->sql_isopen);
    var->isnull = false;
    
    /* Set the magic implicit cursor attribute variable ROWCOUNT to 0 */
    var = (PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]);
    /* reset == true and rowcount == -1, rowcount is set NULL */
    if (FuncValInfo->sql_rowcount == -1) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = FuncValInfo->sql_rowcount;
        var->isnull = false;
    }
}

void SetFuncInfoValue(List* SessionFuncInfo, PLpgSQL_execstate* estate)
{
    ListCell* cell = NULL;
    AutoSessionFuncValInfo* FuncValInfo = NULL;

    foreach(cell, SessionFuncInfo) {
        FuncValInfo = (AutoSessionFuncValInfo*)lfirst(cell);
        if (COMPAT_CURSOR) {
            setCursorAtrValue(estate, FuncValInfo);
        }
        if (!FuncValInfo->sqlcode_isnull) {
            PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[estate->sqlcode_varno]);
            assign_text_var(var, plpgsql_get_sqlstate(FuncValInfo->sqlcode));
            var = (PLpgSQL_var*)(estate->datums[estate->sqlstate_varno]);
            assign_text_var(var, plpgsql_get_sqlstate(FuncValInfo->sqlcode));
        }
    }
}

/* update function info already in auto session by parent session */
void initAutoSessionFuncInfoValue(uint64 sessionId, PLpgSQL_execstate* estate)
{
    SessionPackageRuntime* sessionPkgs = GetSessPkgRuntime(sessionId);

    if (sessionPkgs == NULL) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
        return;
    }
    if (sessionPkgs->funcValInfo == NULL) {
        return;
    }
    SetFuncInfoValue(sessionPkgs->funcValInfo, estate);
}


/* update packages when initializing package in auto session by parent session */
void initAutonomousPkgValue(PLpgSQL_package* targetPkg, uint64 sessionId)
{
    if (u_sess->plsql_cxt.not_found_parent_session_pkgs) {
        return;
    }

    SessionPackageRuntime* sessionPkgs = GetSessPkgRuntime(sessionId);

    if (sessionPkgs == NULL) {
        u_sess->plsql_cxt.not_found_parent_session_pkgs = true;
        return;
    }

    ListCell* cell = NULL;
    PackageRuntimeState* pkgState = NULL;

    foreach(cell, sessionPkgs->runtimes) {
        pkgState = (PackageRuntimeState*)lfirst(cell);
        if (targetPkg->pkg_oid == pkgState->packageId) {
            RestorePkgValuesByPkgState(targetPkg, pkgState, true);
            break;
        }
    }
}

List *processAutonmSessionPkgs(PLpgSQL_function* func, PLpgSQL_execstate* estate, bool isAutonm)
{
    List *autonmsList = NIL;

    uint64 currentSessionId = IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;

    if (IsAutonomousTransaction(func->action->isAutonomous)) {
        /*
         * call after plpgsql_exec_autonm_function(), need restore
         * autonm session pkgs to current session, and remove
         * sessionpkgs from g_instance.global_session_pkg
         */
        uint64 automnSessionId = u_sess->SPI_cxt.autonomous_session->current_attach_sessionid;
        if (func->is_insert_gs_source) {
            /* doing insert gs_source, need do noting */
            g_instance.global_session_pkg->Remove(automnSessionId);
            g_instance.global_session_pkg->Remove(currentSessionId);
            return NIL;
        }

        SessionPackageRuntime* sessionpkgs = g_instance.global_session_pkg->Fetch(automnSessionId);
        RestoreAutonmSessionPkgs(sessionpkgs);
        if (sessionpkgs != NULL) {
            MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
            u_sess->plsql_cxt.storedPortals = CopyPortalDatas(sessionpkgs);
            u_sess->plsql_cxt.portalContext = CopyPortalContexts(sessionpkgs->portalContext);
            autonmsList = CopyFuncInfoDatas(sessionpkgs);
            u_sess->plsql_cxt.call_after_auto = true;
            MemoryContextSwitchTo(oldcontext);
            MemoryContextDelete(sessionpkgs->context);
            g_instance.global_session_pkg->Remove(automnSessionId);
        }
        g_instance.global_session_pkg->Remove(currentSessionId);
    } else {
        /*
         * call after plpgsql_exec_function(), If it is first level
         * autonomous func, need add its all session package values
         * to global, the parent session will fetch the sessionPkgs,
         * and restore package values by it.
         */
        if (u_sess->is_autonomous_session == true && u_sess->SPI_cxt._connected == 0) {
            /* doing insert gs_source, need do noting */
            if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL
                || !u_sess->plsql_cxt.auto_parent_session_pkgs->is_insert_gs_source) {
                BuildSessionPackageRuntimeForParentSession(currentSessionId, estate);
            }
            /* autonomous session will be reused by next autonomous procedure, need clean it */
            if (u_sess->plsql_cxt.auto_parent_session_pkgs != NULL) {
                MemoryContextDelete(u_sess->plsql_cxt.auto_parent_session_pkgs->context);
                u_sess->plsql_cxt.auto_parent_session_pkgs = NULL;
            }
            u_sess->plsql_cxt.not_found_parent_session_pkgs = false;
        }
    }
    return autonmsList;
}

void processAutonmSessionPkgsInException(PLpgSQL_function* func)
{
    uint64 currentSessionId = IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;

    if (IsAutonomousTransaction(func->action->isAutonomous)) {
        /*
         * call after plpgsql_exec_autonm_function(), need restore
         * autonm session pkgs to current session, and remove
         * sessionpkgs from g_instance.global_session_pkg
         */
        if (u_sess->SPI_cxt.autonomous_session == NULL) {
            /* exception before create autonomous_session */
            return;
        }
        uint64 automnSessionId = u_sess->SPI_cxt.autonomous_session->current_attach_sessionid;
        if (func->is_insert_gs_source) {
            /* doing insert gs_source, need do noting */
            g_instance.global_session_pkg->Remove(automnSessionId);
            g_instance.global_session_pkg->Remove(currentSessionId);
            return;
        }

        SessionPackageRuntime* sessionpkgs = g_instance.global_session_pkg->Fetch(automnSessionId);
        RestoreAutonmSessionPkgs(sessionpkgs);
        if (sessionpkgs != NULL) {
            MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
            ReleaseUnusedPortalContext(sessionpkgs->portalContext, true);
            MemoryContextSwitchTo(oldcontext);
            MemoryContextDelete(sessionpkgs->context);
            g_instance.global_session_pkg->Remove(automnSessionId);
        }
        g_instance.global_session_pkg->Remove(currentSessionId);
    } else {
        /*
         * call after plpgsql_exec_function(), If it is first level
         * autonomous func, need add its all session package values
         * to global, the parent session will fetch the sessionPkgs,
         * and restore package values by it.
         */
        if (u_sess->is_autonomous_session == true && u_sess->SPI_cxt._connected == 0) {
            /* doing insert gs_source, need do noting */
            if (u_sess->plsql_cxt.auto_parent_session_pkgs == NULL
                || !u_sess->plsql_cxt.auto_parent_session_pkgs->is_insert_gs_source) {
                BuildSessionPackageRuntimeForParentSession(currentSessionId, NULL);
            }
            /* autonomous session will be reused by next autonomous procedure, need clean it */
            if (u_sess->plsql_cxt.auto_parent_session_pkgs != NULL) {
                MemoryContextDelete(u_sess->plsql_cxt.auto_parent_session_pkgs->context);
                u_sess->plsql_cxt.auto_parent_session_pkgs = NULL;
            }
            u_sess->plsql_cxt.not_found_parent_session_pkgs = false;
        }
    }
}

#ifndef ENABLE_MULTIPLE_NODES
Oid GetOldTupleOid(const char* procedureName, oidvector* parameterTypes, Oid procNamespace,
                          Oid propackageid, Datum* values, Datum parameterModes)
{
    bool enableOutparamOverride = enable_out_param_override();
    if (t_thrd.proc->workingVersionNum < 92470) {
        HeapTuple oldtup = SearchSysCache3(PROCNAMEARGSNSP,
            PointerGetDatum(procedureName),
            values[Anum_pg_proc_proargtypes - 1],
            ObjectIdGetDatum(procNamespace));
        if (!HeapTupleIsValid(oldtup)) {
            return InvalidOid;
        }
        Oid oldTupleOid = HeapTupleGetOid(oldtup);
        ReleaseSysCache(oldtup);
        return oldTupleOid;
    }
    if (enableOutparamOverride) {
        HeapTuple oldtup = NULL;
        oldtup = SearchSysCacheForProcAllArgs(PointerGetDatum(procedureName),
            values[Anum_pg_proc_allargtypes - 1],
            ObjectIdGetDatum(procNamespace),
            ObjectIdGetDatum(propackageid),
            parameterModes);
        if (!HeapTupleIsValid(oldtup)) {
            return InvalidOid;
        }
        Oid oldTupleOid = HeapTupleGetOid(oldtup);
        ReleaseSysCache(oldtup);
        return oldTupleOid;
    } else {
        CatCList* catlist = NULL;
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(procedureName));
        for (int i = 0; i < catlist->n_members; i++) {
            HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
            Oid packageid = InvalidOid;
            if (HeapTupleIsValid(proctup)) {
                Form_pg_proc pform = (Form_pg_proc)GETSTRUCT(proctup);
                Oid oldTupleOid = HeapTupleGetOid(proctup);
                /* compare function's namespace */
                if (pform->pronamespace != procNamespace) {
                    continue;
                }
                bool isNull = false;
                Datum packageIdDatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isNull);
                if (!isNull) {
                    packageid = ObjectIdGetDatum(packageIdDatum);
                }
                if (packageid != propackageid) {
                    continue;
                }
                oidvector* procParaType = ProcedureGetArgTypes(proctup);
                bool result = DatumGetBool(
                    DirectFunctionCall2(oidvectoreq, PointerGetDatum(procParaType),
                                        PointerGetDatum(parameterTypes)));
                if (result) {
                    ReleaseSysCacheList(catlist);
                    return oldTupleOid;
                }
            }
        }
        if (catlist != NULL) {
            ReleaseSysCacheList(catlist);
        }
    }
    return InvalidOid;
}

/*
 * judage the two arglis is same or not
 */
bool isSameArgList(CreateFunctionStmt* stmt1, CreateFunctionStmt* stmt2)
{
    List* argList1 = stmt1->parameters;
    List* argList2 = stmt2->parameters;
    ListCell* cell =  NULL;
    int length1 = list_length(argList1);
    int length2 = list_length(argList2);
    bool enable_outparam_override = enable_out_param_override();
    bool isSameName = true;
    FunctionParameter** arr1 = (FunctionParameter**)palloc0(length1 * sizeof(FunctionParameter*));
    FunctionParameter** arr2 = (FunctionParameter**)palloc0(length2 * sizeof(FunctionParameter*));
    FunctionParameter* fp1 = NULL;
    FunctionParameter* fp2 = NULL;
    int inArgNum1 = 0;
    int inArgNum2 = 0;
    int inLoc1 = 0;
    int inLoc2 = 0;
    int length = 0;
    foreach(cell, argList1) {
        arr1[length] = (FunctionParameter*)lfirst(cell);
        if (arr1[length]->mode != FUNC_PARAM_OUT) {
            inArgNum1++;
        }
        length = length + 1;
    }
    length = 0;
    foreach(cell, argList2) {
        arr2[length] = (FunctionParameter*)lfirst(cell);
        if (arr2[length]->mode != FUNC_PARAM_OUT) {
            inArgNum2++;
        }
        length = length + 1;
    }
    if (!enable_outparam_override) {
        if (inArgNum1 != inArgNum2) {
            return false;
        } else if (inArgNum1 == inArgNum2 && length1 != length2) {
            char message[MAXSTRLEN];
            errno_t rc = sprintf_s(message, MAXSTRLEN, "can not override out param:%s", stmt1->funcname);
            securec_check_ss_c(rc, "", "");
            InsertErrorMessage(message, stmt1->startLineNumber);
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmodule(MOD_PLSQL),
                    errmsg("can not override out param:%s",
                    NameListToString(stmt1->funcname))));
        }
    } else {
        if (length1 != length2) {
            return false;
        }
    }
    for (int i = 0, j = 0; i < length1 || j < length2; i++, j++) {
        if (!enable_outparam_override) {
            fp1 = arr1[inLoc1];
            fp2 = arr2[inLoc2];
        } else {
            fp1 = arr1[i];
            fp2 = arr2[j];
        }
        TypeName* t1 = fp1->argType;
        TypeName* t2 = fp2->argType;
        if (!enable_outparam_override) {
            if (fp1->mode == FUNC_PARAM_OUT && fp2->mode == FUNC_PARAM_OUT) {
                continue;
            } else if (fp1->mode != FUNC_PARAM_OUT && fp2->mode == FUNC_PARAM_OUT) {
                inLoc1++;
                continue;
            } else if (fp1->mode == FUNC_PARAM_OUT && fp2->mode != FUNC_PARAM_OUT) {
                inLoc2++;
                continue;
            } else {
                inLoc1++;
                inLoc2++;
            }
        }
        Oid toid1;
        Oid toid2;
        Type typtup1;
        Type typtup2;
        errno_t rc;
        typtup1 = LookupTypeName(NULL, t1, NULL);
        typtup2 = LookupTypeName(NULL, t2, NULL);
        bool isTableOf1 = false;
        bool isTableOf2 = false;
        Oid baseOid1 = InvalidOid;
        Oid baseOid2 = InvalidOid;
        if (HeapTupleIsValid(typtup1)) {
            toid1 = typeTypeId(typtup1);
            if (((Form_pg_type)GETSTRUCT(typtup1))->typtype == TYPTYPE_TABLEOF) {
                baseOid1 = ((Form_pg_type)GETSTRUCT(typtup1))->typelem;
                isTableOf1 = true;
            }
            ReleaseSysCache(typtup1);
        } else {
            toid1 = findPackageParameter(strVal(linitial(t1->names)));
            if (!OidIsValid(toid1)) {
                char message[MAXSTRLEN];
                rc = sprintf_s(message, MAXSTRLEN, "type is not exists  %s.", fp1->name);
                securec_check_ss_c(rc, "", "");
                InsertErrorMessage(message, stmt1->startLineNumber);
                ereport(ERROR,
                    (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("type is not exists  %s.", fp1->name),
                        errdetail("CommandType: %s", fp1->name), errcause("System error."),
                        erraction("Contact Huawei Engineer.")));
            }
        }
        if (HeapTupleIsValid(typtup2)) {
            toid2 = typeTypeId(typtup2);
            if (((Form_pg_type)GETSTRUCT(typtup2))->typtype == TYPTYPE_TABLEOF) {
                baseOid2 = ((Form_pg_type)GETSTRUCT(typtup2))->typelem;
                isTableOf2 = true;
            }
            ReleaseSysCache(typtup2);
        } else {
            toid2 = findPackageParameter(strVal(linitial(t2->names)));
            if (!OidIsValid(toid2)) {
                char message[MAXSTRLEN];
                rc = sprintf_s(message, MAXSTRLEN, "type is not exists  %s.", fp2->name);
                securec_check_ss_c(rc, "", "");
                InsertErrorMessage(message, stmt1->startLineNumber);
                ereport(ERROR,
                    (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("type is not exists  %s.", fp2->name),
                        errdetail("CommandType: %s", fp2->name), errcause("System error."),
                        erraction("Contact Huawei Engineer.")));
            }
        }
        /* If table of type shold check its base type */
        if (isTableOf1 == isTableOf2 && isTableOf1 == true) {
            if (baseOid1 != baseOid2 || fp1->mode != fp2->mode) {
                pfree(arr1);
                pfree(arr2);
                return false;
            }
        } else if (toid1 != toid2 || fp1->mode != fp2->mode) {
            pfree(arr1);
            pfree(arr2);
            return false;
        }
        if (fp1->name == NULL || fp2->name == NULL) {
            char message[MAXSTRLEN];
            rc = sprintf_s(message, MAXSTRLEN, "type is not exists.");
            securec_check_ss_c(rc, "", "");
            InsertErrorMessage(message, stmt1->startLineNumber);
            ereport(ERROR,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("type is not exists."),
                    errdetail("CommandType."), errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
        }
        if (strcmp(fp1->name, fp2->name) != 0) {
            isSameName = false;
        }
    }
    pfree(arr1);
    pfree(arr2);
    /* function delcare in package specification and define in package body must be same */
    if (!isSameName && (stmt1->isFunctionDeclare^stmt2->isFunctionDeclare)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_PLSQL),
                errmsg("function declared in package specification and "
                "package body must be the same, function: %s",
                NameListToString(stmt1->funcname))));
    }
    return true;
}

#endif
