/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * pg_synonym.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/pg_synonym.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup.h"
#include "access/sysattr.h"
#include "lib/stringinfo.h"
#include "catalog/dependency.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/indexing.h"
#include "catalog/pg_database.h"
#include "catalog/pg_synonym.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "storage/proc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/catcache.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "miscadmin.h"
#include "client_logic/client_logic.h"

static Oid SynonymCreate(
    Oid synNamespace, const char* synName, Oid synOwner, const char* objSchema, const char* objName, bool replace);
static void SynonymDrop(Oid synNamespace, char* synName, DropBehavior behavior, bool missing);

void CheckCreateSynonymPrivilege(Oid synNamespace, const char* synName)
{
    if (!isRelSuperuser() &&
        (synNamespace == PG_CATALOG_NAMESPACE ||
        synNamespace == PG_PUBLIC_NAMESPACE ||
        synNamespace == PG_DB4AI_NAMESPACE)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create synonym \"%s\"", synName),
                errhint("must be %s to create a synonym in %s schema.",
                    g_instance.attr.attr_security.enablePrivilegesSeparate ? "initial user" : "sysadmin",
                    get_namespace_name(synNamespace))));
    }

    if (!IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        !g_instance.attr.attr_common.allow_create_sysobject &&
        IsSysSchema(synNamespace)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create synonym \"%s\"", synName),
                errhint("not allowd to create a synonym in %s schema when allow_create_sysobject is off.",
                    get_namespace_name(synNamespace))));
    }
}

/*
 * CREATE SYNONYM
 */
void CreateSynonym(CreateSynonymStmt* stmt)
{
    Oid synNamespace;
    char* synName = NULL;
    char* objSchema = NULL;
    char* objName = NULL;
    AclResult aclResult;

    if (u_sess->proc_cxt.MyDatabaseId == TemplateDbOid) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Using CREATE SYNONYM is forbidden in template database.")));
    }

    /* Convert list of synonym names to a synName and a synNamespace. */
    synNamespace = QualifiedNameGetCreationNamespace(stmt->synName, &synName);
    Assert(OidIsValid(synNamespace));

    /* Check we have creation rights in namespace. */
    aclResult = pg_namespace_aclcheck(synNamespace, GetUserId(), ACL_CREATE);
    bool anyResult = false;
    if (aclResult != ACLCHECK_OK && !IsSysSchema(synNamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), CREATE_ANY_SYNONYM, false);
    }
    if (aclResult != ACLCHECK_OK && !anyResult) {
        aclcheck_error(aclResult, ACL_KIND_NAMESPACE, get_namespace_name(synNamespace));
    }

    CheckCreateSynonymPrivilege(synNamespace, synName);
    /* Deconstruct the referenced qualified-name. */
    DeconstructQualifiedName(stmt->objName, &objSchema, &objName);

    /* Using the default creation namespace. */
    if (objSchema == NULL) {
        objSchema = get_namespace_name(GetOidBySchemaName());
    }

    /* 
     * Check synonym name to ensure that it doesn't conflict with existing view, table, function, and procedure.
     */
    if (get_relname_relid(synName, synNamespace) != InvalidOid || get_func_oid(synName, synNamespace, NULL) != InvalidOid) {
        ereport(ERROR, (errmsg("synonym name is already used by an existing object")));
    }

    if (IsFullEncryptedRel(objSchema, objName)) {
        ereport(ERROR, (errmsg("Unsupport to CREATE SYNONYM for encryption table.")));
    } else if (IsFuncProcOnEncryptedRel(objSchema, objName)) {
        ereport(ERROR, (errmsg("Unsupport to CREATE SYNONYM for encryption procedure or function.")));
    } else {
        /* Main entry to create a synonym */
        SynonymCreate(synNamespace, synName, GetUserId(), objSchema, objName, stmt->replace);
    }
}

/*
 * DROP SYNONYM
 */
void DropSynonym(DropSynonymStmt* stmt)
{
    Oid synNamespace;
    char* synName = NULL;
    AclResult aclResult;

    /* Convert list of synonym names to a synName and a synNamespace. */
    synNamespace = QualifiedNameGetCreationNamespace(stmt->synName, &synName);
    Assert(OidIsValid(synNamespace));

    /* Check we have usage privilege in namespace */
    aclResult = pg_namespace_aclcheck(synNamespace, GetUserId(), ACL_USAGE);
    if (aclResult != ACLCHECK_OK) {
        aclcheck_error(aclResult, ACL_KIND_NAMESPACE, get_namespace_name(synNamespace));
    }

    SynonymDrop(synNamespace, synName, stmt->behavior, stmt->missing);
}

/*
 * SynonymCreate
 * Define a new synonym.
 *
 * Form a synonym tuple and then insert or update it into system table pg_synonym.
 * And then return the OID assigned to the new synonym object.
 */
static Oid SynonymCreate(
    Oid synNamespace, const char* synName, Oid synOwner, const char* objSchema, const char* objName, bool replace)
{
    Relation rel = NULL;
    TupleDesc tupDesc;
    HeapTuple tuple = NULL;
    HeapTuple oldTuple = NULL;
    bool nulls[Natts_pg_synonym];
    Datum values[Natts_pg_synonym];
    bool replaces[Natts_pg_synonym];
    NameData synonymName;
    NameData objectSchemaName;
    NameData objectName;
    Oid synOid;
    ObjectAddress myself;
    ObjectAddress referenced;
    bool isUpdate = false;

    /* In principle, these values are valid. */
    Assert(OidIsValid(synNamespace));
    Assert(OidIsValid(synOwner));
    Assert(objSchema != NULL);
    Assert(objName != NULL);

    /* open pg_synonym */
    rel = heap_open(PgSynonymRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* initialize nulls, values and replaces. */
    errno_t rc = EOK;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "", "");

    /* form a tuple */
    (void)namestrcpy(&synonymName, synName);
    values[Anum_pg_synonym_synname - 1] = NameGetDatum(&synonymName);
    values[Anum_pg_synonym_synnamespace - 1] = ObjectIdGetDatum(synNamespace);
    values[Anum_pg_synonym_synowner - 1] = ObjectIdGetDatum(synOwner);
    (void)namestrcpy(&objectSchemaName, objSchema);
    values[Anum_pg_synonym_synobjschema - 1] = NameGetDatum(&objectSchemaName);
    (void)namestrcpy(&objectName, objName);
    values[Anum_pg_synonym_synobjname - 1] = NameGetDatum(&objectName);

    /* search syscache, if not existed, ok. */
    oldTuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace));
    if (HeapTupleIsValid(oldTuple)) {
        /* a existed tuple, if replace, assign replaces[] and modify tuple. */
        if (!replace) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("synonym \"%s\" already exists", synName)));
        }

        replaces[Anum_pg_synonym_synobjschema - 1] = true;
        replaces[Anum_pg_synonym_synobjname - 1] = true;

        tuple = heap_modify_tuple(oldTuple, tupDesc, values, nulls, replaces);
        simple_heap_update(rel, &tuple->t_self, tuple);
        ReleaseSysCache(oldTuple);

        synOid = HeapTupleGetOid(tuple);
        isUpdate = true;
    } else {
        /* a new tuple, directly insert. */
        tuple = heap_form_tuple(tupDesc, values, nulls);
        synOid = simple_heap_insert(rel, tuple);
        isUpdate = false;
    }

    /* update the index if any */
    CatalogUpdateIndexes(rel, tuple);

    ObjectAddressSet(myself, PgSynonymRelationId, HeapTupleGetOid(tuple));

    /* record the dependencies, for the first create. */
    if (!isUpdate) {
        /* dependency on namespace of synonym object */
        referenced.classId = NamespaceRelationId;
        referenced.objectId = synNamespace;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        /* dependency on owner of synonym object */
        recordDependencyOnOwner(myself.classId, myself.objectId, synOwner);
    }

    recordDependencyOnCurrentExtension(&myself, isUpdate);

    heap_freetuple(tuple);
    heap_close(rel, RowExclusiveLock);

    return synOid;
}

/*
 * SynonymDrop
 * Drop a synonym.
 *
 * Search pg_synonym given synonym info and delete the found synonym object.
 * If more dependencies has existed, they all will be deleted when CASCADE specified.
 */
static void SynonymDrop(Oid synNamespace, char* synName, DropBehavior behavior, bool missing)
{
    HeapTuple tuple = NULL;
    ObjectAddress object;

    Assert(OidIsValid(synNamespace));

    tuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace));
    if (!HeapTupleIsValid(tuple)) {
        if (!missing) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("synonym \"%s\" does not exist", synName)));
        } else {
            ereport(NOTICE, (errmsg("synonym \"%s\" does not exist, skipping", synName)));
            return;
        }
    }

    Oid synOid = HeapTupleGetOid(tuple);
    Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(tuple);
    /* Allow DROP to either synonym owner or schema owner or user having DROP ANY SYNONYM privilege. */
    bool ownerResult = pg_synonym_ownercheck(synOid, GetUserId()) ||
        pg_namespace_ownercheck(synForm->synnamespace, GetUserId());
    bool anyResult = false;
    if (!ownerResult && !IsSysSchema(synForm->synnamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), DROP_ANY_SYNONYM, false);
    }
    if (!ownerResult && !anyResult) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, NameStr(synForm->synname));
    }
    ReleaseSysCache(tuple);

    /*
     * Do the deletion
     */
    object.classId = PgSynonymRelationId;
    object.objectId = synOid;
    object.objectSubId = 0;

    performDeletion(&object, behavior, 0);
}

/*
 * ALTER Synonym name OWNER TO newowner
 */
ObjectAddress AlterSynonymOwner(List* name, Oid newOwnerId)
{
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    Oid synNamespace, synOid;
    char* synName = NULL;
    ObjectAddress address;

    /* Convert list of synonym names to a synName and a synNamespace. */
    synNamespace = QualifiedNameGetCreationNamespace(name, &synName);
    AssertEreport(OidIsValid(synNamespace), MOD_EXECUTOR, "namespace is invalid.");

    rel = heap_open(PgSynonymRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace));
    if (!HeapTupleIsValid(tuple)) {
        /* Clean up and report error. */
        heap_close(rel, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("synonym \"%s\" does not exist", synName)));
    }
    synOid = HeapTupleGetOid(tuple);
    Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(tuple);

    /*
     * If the new owner is the same as the existing owner, consider the command to have succeeded.
     * Here, only user with sysadmin privilege can do alter.
     * ps. This is for dump restoration purposes.
     */
    if (synForm->synowner != newOwnerId) {
        if (!superuser()) {
            heap_freetuple_ext(tuple);
            heap_close(rel, RowExclusiveLock);
            ereport(ERROR,
                (errmodule(MOD_EC),
                    errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to change owner of synonym \"%s\"", synName),
                    errhint("Must be system admin to change owner of a synonym.")));
        }

        /* New owner must have CREATE privilege on namespace */
        if (OidIsValid(synForm->synnamespace)) {
            AclResult aclResult = pg_namespace_aclcheck(synForm->synnamespace, newOwnerId, ACL_CREATE);
            if (aclResult != ACLCHECK_OK) {
                aclcheck_error(aclResult, ACL_KIND_NAMESPACE, get_namespace_name(synForm->synnamespace));
            }
        }

        /* Change its owner */
        synForm->synowner = newOwnerId;

        simple_heap_update(rel, &tuple->t_self, tuple);
        CatalogUpdateIndexes(rel, tuple);

        /* Update owner dependency reference. */
        changeDependencyOnOwner(PgSynonymRelationId, synOid, newOwnerId);
    }

    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
    ObjectAddressSet(address, PgSynonymRelationId, synOid);
    return address;
}

/*
 * AlterSynonymOwnerByOid - ALTER Synonym OWNER TO newowner by Oid
 * This is currently only used to propagate ALTER PACKAGE OWNER to a
 * package. Package will build Synonym for ref cursor type.
 * It assumes the caller has done all needed checks.
 */
void AlterSynonymOwnerByOid(Oid synonymOid, Oid newOwnerId)
{
    HeapTuple tuple = NULL;
    Relation rel = NULL;

    rel = heap_open(PgSynonymRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(SYNOID, ObjectIdGetDatum(synonymOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for synonym %u", synonymOid)));
    }
    Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(tuple);

    /*
     * If the new owner is the same as the existing owner, consider the command to have succeeded.
     * ps. This is for dump restoration purposes.
     */
    if (synForm->synowner != newOwnerId) {
        /* Change its owner */
        synForm->synowner = newOwnerId;

        simple_heap_update(rel, &tuple->t_self, tuple);
        CatalogUpdateIndexes(rel, tuple);

        /* Update owner dependency reference. */
        changeDependencyOnOwner(PgSynonymRelationId, HeapTupleGetOid(tuple), newOwnerId);
    }

    ReleaseSysCache(tuple);
    heap_close(rel, NoLock);
}

/*
 * RemoveSynonymById
 * Given synonym oid, remove the synonym tuple.
 *
 * Search for synonym tuple given synonym oid, and then remove it.
 */
void RemoveSynonymById(Oid synonymOid)
{
    Relation rel;
    HeapTuple tuple = NULL;

    Assert(OidIsValid(synonymOid));

    /* open pg_synonym */
    rel = heap_open(PgSynonymRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(SYNOID, ObjectIdGetDatum(synonymOid));
    /* search for the target tuple */
    if (!HeapTupleIsValid(tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for synonym %u", synonymOid)));
    }

    simple_heap_delete(rel, &tuple->t_self);

    ReleaseSysCache(tuple);
    heap_close(rel, RowExclusiveLock);
}

/*
 * SearchReferencedObject
 *
 * Given synonym name and its namespace, search for and construct its referenced object.
 */
RangeVar* SearchReferencedObject(const char* synName, Oid synNamespace)
{
    HeapTuple synTuple;
    RangeVar* objVar = NULL;

    /* Do nothing during inplace upgrade or initdb. */
    if (!IsNormalProcessingMode() || IsInitdb || t_thrd.proc->workingVersionNum < SYNONYM_VERSION_NUM)
        return NULL;

    /* Sometimes, maybe pg_synonym is not existed, so do nothing. */
    if (!SearchSysCacheExists1(RELOID, PgSynonymRelationId))
        return NULL;

    synTuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace));
    if (HeapTupleIsValid(synTuple)) {
        /* found and construct its referenced RangeVar including schemaname and objname. */
        Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(synTuple);
        objVar = makeRangeVar(NameStr(synForm->synobjschema), NameStr(synForm->synobjname), -1);
        ReleaseSysCache(synTuple);
    }

    return objVar;
}

/*
 * IsSynonymExist
 *
 * Given opened relation, check it and generate the detail info.
 */
static bool IsSynonymExist(const RangeVar* relation)
{
    Oid synNamespace;
    char* synName = NULL;

    /* Do nothing during inplace upgrade or initdb. */
    if (!IsNormalProcessingMode() || IsInitdb || t_thrd.proc->workingVersionNum < SYNONYM_VERSION_NUM)
        return false;

    synNamespace = get_namespace_oid(relation->schemaname, true);
    synName = relation->relname;

    if (SearchSysCacheExists2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace))) {
        return true;
    }
    return false;
}

/*
 * CheckReferenceObject
 *
 * Given opened relation, check it and generate the detail info.
 */
char* CheckReferencedObject(Oid relOid, RangeVar* objVar, const char* synName)
{
    StringInfoData detail;
    initStringInfo(&detail);

    /* if referenced object is not found, do something. */
    if (!OidIsValid(relOid)) {
        if (IsSynonymExist(objVar)) {
            /* append details info when it is another synonym which has not been supported yet. */
            appendStringInfo(&detail,
                _("Maybe you want to use synonym to reference another synonym object, but it is not yet supported."));
        } else {
            /* invalid transltion, do not report right now and record in detial. */
            appendStringInfo(&detail, "translation for synonym \"%s\" is no longer valid", synName);
        }
        return detail.data;
    }

    /* supported and unsupported cases check. */
    switch (get_rel_relkind(relOid)) {
        case RELKIND_RELATION:
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
        case RELKIND_FOREIGN_TABLE:
        case RELKIND_STREAM:
        case RELKIND_MATVIEW:
            break;
        case RELKIND_COMPOSITE_TYPE:
            appendStringInfo(
                &detail, _("Maybe you want to use synonym to reference a type object, but it is not yet supported."));
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            appendStringInfo(&detail,
                _("Maybe you want to use synonym to reference a (large) sequence object, "
                  "but it is not yet supported."));
            break;
        default:
            appendStringInfo(&detail,
                _("Maybe you want to use synonym to reference a unsupported object, but it is not yet supported."));
            break;
    }
    return detail.data;
}

/*
 * GetSynonymOid
 *
 * Given a synonym name and its namespace, look up its Oid.
 */
Oid GetSynonymOid(const char* synName, Oid synNamespace, bool missing)
{
    Oid synOid = InvalidOid;

    Assert(OidIsValid(synNamespace));

    synOid = GetSysCacheOid2(SYNONYMNAMENSP, PointerGetDatum(synName), ObjectIdGetDatum(synNamespace));
    if (!OidIsValid(synOid) && !missing) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("synonym \"%s\" does not exist", synName)));
    }

    return synOid;
}

/*
 * GetQualifiedSynonymName
 *
 * Given a synonym OID, look up its qualified name.
 */
char* GetQualifiedSynonymName(Oid synOid, bool qualified)
{
    char* synName = NULL;
    char* synSchema = NULL;
    Assert(OidIsValid(synOid));

    GetSynonymAndSchemaName(synOid, &synName, &synSchema);

    return qualified ? quote_qualified_identifier(synSchema, synName) : synName;
}

/*
 * GetSynonymAndSchemaName
 *
 * Given a synonym OID, look up its name and its schema name and store them.
 */
void GetSynonymAndSchemaName(Oid synOid, char** synName_p, char** synSchema_p)
{
    HeapTuple synTuple = SearchSysCache1(SYNOID, ObjectIdGetDatum(synOid));
    if (HeapTupleIsValid(synTuple)) {
        Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(synTuple);
        *synName_p = pstrdup(NameStr(synForm->synname));
        *synSchema_p = get_namespace_name(synForm->synnamespace);

        ReleaseSysCache(synTuple);
    }
}
