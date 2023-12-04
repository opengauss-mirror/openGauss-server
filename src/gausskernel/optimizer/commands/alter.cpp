/* -------------------------------------------------------------------------
 *
 * alter.cpp
 *	  Drivers for generic alter commands
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/alter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_synonym.h"
#include "commands/alter.h"
#include "commands/collationcmds.h"
#include "commands/conversioncmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/directory.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/subscriptioncmds.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "gs_policy/gs_policy_masking.h"
#include "catalog/gs_dependencies_fn.h"

/*
 * Executes an ALTER OBJECT / RENAME TO statement.	Based on the object
 * type, the function appropriate to that type is executed.
 */
static void
report_name_conflict(Oid classId, const char *name)
{
    char *msgfmt;    
    switch (classId) {
            
        case EventTriggerRelationId:
            msgfmt = gettext_noop("event trigger \"%s\" already exists");
            break;
        case ForeignDataWrapperRelationId:
            msgfmt = gettext_noop("foreign-data wrapper \"%s\" already exists");
            break;
        case ForeignServerRelationId:
            msgfmt = gettext_noop("server \"%s\" already exists");
            break;
        case LanguageRelationId:
            msgfmt = gettext_noop("language \"%s\" already exists");
            break;
        case PublicationRelationId:
            msgfmt = gettext_noop("publication \"%s\" already exists");
            break;
        case SubscriptionRelationId:
            msgfmt = gettext_noop("subscription \"%s\" already exists");
            break;
        default:
            elog(ERROR, "unsupported object class %u", classId);
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
             errmsg(msgfmt, name)));
}

static void
report_namespace_conflict(Oid classId, const char *name, Oid nspOid)
{
    char   *msgfmt;
    Assert(OidIsValid(nspOid));

    switch (classId) {
        case ConversionRelationId:
            Assert(OidIsValid(nspOid));
            msgfmt = gettext_noop("conversion \"%s\" already exists in schema \"%s\"");
            break;
        case TSParserRelationId:
            Assert(OidIsValid(nspOid));
            msgfmt = gettext_noop("text search parser \"%s\" already exists in schema \"%s\"");
            break;
        case TSDictionaryRelationId:
            Assert(OidIsValid(nspOid));
            msgfmt = gettext_noop("text search dictionary \"%s\" already exists in schema \"%s\"");
            break;
        case TSTemplateRelationId:
            Assert(OidIsValid(nspOid));
            msgfmt = gettext_noop("text search template \"%s\" already exists in schema \"%s\"");
            break;
        case TSConfigRelationId:
            Assert(OidIsValid(nspOid));
            msgfmt = gettext_noop("text search configuration \"%s\" already exists in schema \"%s\"");
            break;
        default:
            elog(ERROR, "unsupported object class %u", classId);
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
             errmsg(msgfmt, name, get_namespace_name(nspOid))));
}

/*
 * AlterObjectRename_internal
 *
 * Generic function to rename the given object, for simple cases (won't
 * work for tables, nor other cases where we need to do more than change
 * the name column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objectId: OID of object to be renamed
 * new_name: CString representation of new name
 */
static void
AlterObjectRename_internal(Relation rel, Oid objectId, const char *new_name)
{
    Oid         classId = RelationGetRelid(rel);
    int         oidCacheId = get_object_catcache_oid(classId);
    int         nameCacheId = get_object_catcache_name(classId);
    AttrNumber  Anum_name = get_object_attnum_name(classId);
    AttrNumber  Anum_namespace = get_object_attnum_namespace(classId);

    HeapTuple   oldtup;
    HeapTuple   newtup;
    bool        isnull;
    Oid         namespaceId;
    Oid         userId;
    char       *old_name;
    AclResult   aclresult;
    Datum      *values;
    bool       *nulls;
    bool       *replaces;
    NameData    nameattrdata;
    Datum       datum;

    oldtup = SearchSysCache1(oidCacheId, ObjectIdGetDatum(objectId));
    if (!HeapTupleIsValid(oldtup))
        elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
             objectId, RelationGetRelationName(rel));

    datum = heap_getattr(oldtup, Anum_name,
                         RelationGetDescr(rel), &isnull);
    Assert(!isnull);
    old_name = NameStr(*(DatumGetName(datum)));

    /* Get OID of namespace */
    if (Anum_namespace > 0) {
        datum = heap_getattr(oldtup, Anum_namespace,
                             RelationGetDescr(rel), &isnull);
        Assert(!isnull);
        namespaceId = DatumGetObjectId(datum);
    }
    else
        namespaceId = InvalidOid;

    /* Permission checks ... superusers can always do it */
    if (!superuser()) {
        ObjectType objType = get_object_type(classId, objectId);
        if (objType == OBJECT_TSTEMPLATE) {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to rename text search templates")));
        } else if (objType ==  OBJECT_TSPARSER) {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to rename text search parser")));
        }            

        userId = GetUserId();
        switch (objType) {
            /*OBJECT_AGGREGATE's classid is ProcedureRelationId, so objType should be OBJECT_FUNCTION*/
            case OBJECT_FUNCTION:
                if (pg_proc_aclcheck(objectId, userId, ACL_ALTER) != ACLCHECK_OK &&
                       !pg_proc_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, old_name);
                break;   
            case OBJECT_COLLATION:
                if (pg_collation_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION, old_name);
                break;
            case OBJECT_CONVERSION:
                if (pg_conversion_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONVERSION, old_name);
                break;
            case OBJECT_EVENT_TRIGGER:
                if (!pg_event_trigger_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EVENT_TRIGGER, old_name);
                break;
            case OBJECT_FDW:
                if (!pg_foreign_data_wrapper_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FDW, old_name);
                break;
            case OBJECT_FOREIGN_SERVER:
                 if (pg_foreign_server_aclcheck(objectId, userId, ACL_ALTER) != ACLCHECK_OK &&
                         !pg_foreign_server_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, old_name);
                break;
            case OBJECT_OPCLASS:
                if (!pg_opclass_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS, old_name);
                break;
            case OBJECT_OPFAMILY:
                if (!pg_opfamily_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPFAMILY, old_name);
                break;
            case OBJECT_LANGUAGE:
                if (pg_language_aclcheck(objectId, userId, ACL_ALTER) != ACLCHECK_OK &&
                        !pg_language_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_LANGUAGE, old_name);
                break;
            case OBJECT_TSDICTIONARY:
                if (!pg_ts_dict_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSDICTIONARY, old_name);
                break;
            case OBJECT_TSCONFIGURATION:
                if (!pg_ts_config_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSCONFIGURATION, old_name);
                break;    
            case OBJECT_PUBLICATION:
                if (!pg_publication_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PUBLICATION, old_name);
                break;
            case OBJECT_SUBSCRIPTION:
                if (!pg_subscription_ownercheck(objectId, userId))
                    aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, old_name);
                break;
            default: {
                    ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized object type: %d", (int)objType)));
            } break;
        }        
        /* User must have CREATE privilege on the namespace */
        if (OidIsValid(namespaceId)) {
            aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
                                              ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
                               get_namespace_name(namespaceId));
        }
    }
 
    /*
      * Check for duplicate name (more friendly than unique-index failure).
      * Since this is just a friendliness check, we can just skip it in cases
      * where there isn't suitable support.
      */
    if (classId == ProcedureRelationId) {
        Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(oldtup);
 
        IsThereFunctionInNamespace(new_name, proc->pronargs,
                                   &(proc->proargtypes), proc->pronamespace);
    }
    else if (classId == CollationRelationId) {
        Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(oldtup);
 
        IsThereCollationInNamespace(new_name, coll->collnamespace);
    }
    else if (classId == OperatorClassRelationId) {
        Form_pg_opclass opc = (Form_pg_opclass) GETSTRUCT(oldtup);
 
        IsThereOpClassInNamespace(new_name, opc->opcmethod,
                                  opc->opcnamespace);
    }
    else if (classId == OperatorFamilyRelationId) {
        Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(oldtup);
 
        IsThereOpFamilyInNamespace(new_name, opf->opfmethod,
                                   opf->opfnamespace);
    }
	else if (classId == SubscriptionRelationId) {
		if (SearchSysCacheExists2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId,
								  CStringGetDatum(new_name)))
			report_name_conflict(classId, new_name);
	}
    else if (nameCacheId >= 0) {
        if (OidIsValid(namespaceId)) {
            if (SearchSysCacheExists2(nameCacheId,
                                      CStringGetDatum(new_name),
                                      ObjectIdGetDatum(namespaceId)))
                report_namespace_conflict(classId, new_name, namespaceId);
        }
        else {
            if (SearchSysCacheExists1(nameCacheId,
                                      CStringGetDatum(new_name)))
                report_name_conflict(classId, new_name);
        }
    }
 
    /* Build modified tuple */
    values = (Datum*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
    nulls = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    replaces = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    namestrcpy(&nameattrdata, new_name);
    values[Anum_name - 1] = NameGetDatum(&nameattrdata);
    replaces[Anum_name - 1] = true;
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
                               values, nulls, replaces);
 
    /* Perform actual update */
    simple_heap_update(rel, &oldtup->t_self, newtup);
    CatalogUpdateIndexes(rel, newtup);
 
    /* Release memory */
    pfree(values);
    pfree(nulls);
    pfree(replaces);
    heap_freetuple(newtup);
 
    ReleaseSysCache(oldtup);
}
        
/*
 * Executes an ALTER OBJECT / RENAME TO statement. Based on the object
 * type, the function appropriate to that type is executed.
 */
ObjectAddress
ExecRenameStmt(RenameStmt *stmt)
{
    ObjectAddress address;
    switch (stmt->renameType) {
        case OBJECT_TABCONSTRAINT:
        case OBJECT_DOMCONSTRAINT:
            return RenameConstraint(stmt);

        case OBJECT_DATABASE:
            return RenameDatabase(stmt->subname, stmt->newname);

        case OBJECT_PARTITION:
            return renamePartition(stmt);

        case OBJECT_PARTITION_INDEX:
            return renamePartitionIndex(stmt);

        case OBJECT_RLSPOLICY:
            return RenameRlsPolicy(stmt);

        case OBJECT_ROLE:
            return RenameRole(stmt->subname, stmt->newname);

        case OBJECT_USER: {
            address = RenameRole(stmt->subname, stmt->newname);

            /*
             * Rename user need rename the schema that has the same name which
             * owned by the user.
             */
            RenameSchema(stmt->subname, stmt->newname);
            return address;
        }    
        case OBJECT_SCHEMA:
            return RenameSchema(stmt->subname, stmt->newname);

        case OBJECT_TABLESPACE:
            return RenameTableSpace(stmt->subname, stmt->newname);

        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_VIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_MATVIEW:
        case OBJECT_INDEX:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
            return RenameRelation(stmt);

        case OBJECT_COLUMN:
        case OBJECT_ATTRIBUTE:
            return renameatt(stmt);

        case OBJECT_TRIGGER:
            return renametrig(stmt);

        case OBJECT_DOMAIN:
        case OBJECT_TYPE:
            return RenameType(stmt);
        case OBJECT_FUNCTION:
            return RenameFunction(stmt->object, stmt->objarg, stmt->newname);
        case OBJECT_AGGREGATE:
        case OBJECT_COLLATION:
        case OBJECT_CONVERSION:
        case OBJECT_EVENT_TRIGGER:
        case OBJECT_FDW:
        case OBJECT_FOREIGN_SERVER:
        case OBJECT_OPCLASS:
        case OBJECT_OPFAMILY:
        case OBJECT_LANGUAGE:
        case OBJECT_TSCONFIGURATION:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSPARSER:
        case OBJECT_TSTEMPLATE:
        case OBJECT_PUBLICATION:
        case OBJECT_SUBSCRIPTION:
            {
                ObjectAddress   address;
                Relation        catalog;
                Relation        relation;
                address = get_object_address(stmt->renameType,
                                             stmt->object, stmt->objarg,
                                             &relation,
                                             AccessExclusiveLock, false);
                Assert(relation == NULL);
                catalog = heap_open(address.classId, RowExclusiveLock);
                AlterObjectRename_internal(catalog,
                                           address.objectId,
                                           stmt->newname);
                heap_close(catalog, RowExclusiveLock);
 
                return address;
            }
            
        case OBJECT_DATA_SOURCE:
            return RenameDataSource(stmt->subname, stmt->newname);

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized rename stmt type: %d", (int)stmt->renameType)));
            return InvalidObjectAddress;
    }
    return InvalidObjectAddress;
}

/*
 * Executes an ALTER OBJECT / SET SCHEMA statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
ObjectAddress ExecAlterObjectSchemaStmt(AlterObjectSchemaStmt* stmt, ObjectAddress *oldSchemaAddr)
{
    ObjectAddress address;
    Oid         oldNspOid; 
    switch (stmt->objectType) {
        case OBJECT_EXTENSION:
            address = AlterExtensionNamespace(stmt->object, stmt->newschema);
            break;
        case OBJECT_OPERATOR:
            address = AlterOperatorNamespace(stmt->object, stmt->objarg, stmt->newschema);
            break;
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_TABLE:
        case OBJECT_VIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_MATVIEW:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
            if (stmt->objectType == OBJECT_STREAM)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("target table is a stream")));
             address = AlterTableNamespace(stmt, oldSchemaAddr ? &oldNspOid : NULL);
            break;

            /* generic code path */
        case OBJECT_AGGREGATE:
        case OBJECT_COLLATION:
        case OBJECT_CONVERSION:
        case OBJECT_FUNCTION:
        case OBJECT_OPCLASS:
        case OBJECT_OPFAMILY:
        case OBJECT_TSCONFIGURATION:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSPARSER:
        case OBJECT_TSTEMPLATE:
            {
                Relation    catalog;
                Relation    relation;
                Oid         classId;
                Oid         nspOid;
                address = get_object_address(stmt->objectType,
                                             stmt->object,
                                             stmt->objarg,
                                             &relation,
                                             AccessExclusiveLock,
                                             false);
                Assert(relation == NULL);
                classId = address.classId;
                nspOid = LookupCreationNamespace(stmt->newschema);
                if (stmt->objectType == OBJECT_FUNCTION) {
                    /* 
                    * Check function name to ensure that it doesn't conflict with existing synonym.
                    */

                    Relation procRel = heap_open(ProcedureRelationId, RowExclusiveLock);

                    HeapTuple tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(address.objectId));
                    if (!HeapTupleIsValid(tup))
                        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", address.objectId)));
                    Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(tup);
                    heap_close(procRel, RowExclusiveLock);
                    if (!IsInitdb && GetSynonymOid(NameStr(proc->proname), nspOid, true) != InvalidOid) {

                        ereport(ERROR,
                                (errmsg("function name is already used by an existing synonym in schema \"%s\"",
                                    get_namespace_name(nspOid))));
                    }
                }
                catalog = heap_open(classId, RowExclusiveLock);
                oldNspOid = AlterObjectNamespace_internal(catalog, address.objectId,
                                                          nspOid);
                heap_close(catalog, RowExclusiveLock);
            }
            break;
            
        case OBJECT_TYPE:
        case OBJECT_DOMAIN:
            address = AlterTypeNamespace(stmt->object, stmt->newschema, stmt->objectType);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized AlterObjectSchemaStmt type: %d", (int)stmt->objectType)));
            return InvalidObjectAddress; 
    }
    return address;
}

/*
 * Change an object's namespace given its classOid and object Oid.
 *
 * Objects that don't have a namespace should be ignored.
 *
 * This function is currently used only by ALTER EXTENSION SET SCHEMA,
 * so it only needs to cover object types that can be members of an
 * extension, and it doesn't have to deal with certain special cases
 * such as not wanting to process array types --- those should never
 * be direct members of an extension anyway.
 *
 * Returns the OID of the object's previous namespace, or InvalidOid if
 * object doesn't have a schema.
 */
Oid AlterObjectNamespace_oid(Oid classId, Oid objid, Oid nspOid, ObjectAddresses* objsMoved)
{
    Oid oldNspOid = InvalidOid;
    ObjectAddress dep;

    dep.classId = classId;
    dep.objectId = objid;
    dep.objectSubId = 0;

    switch (getObjectClass(&dep)) {
        case OCLASS_CLASS: {
            Relation rel;

            rel = relation_open(objid, AccessExclusiveLock);
            oldNspOid = RelationGetNamespace(rel);

            AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved);

            relation_close(rel, NoLock);
            break;
        }

        case OCLASS_PROC:
            oldNspOid = AlterFunctionNamespace_oid(objid, nspOid);
            break;

        case OCLASS_TYPE:
            oldNspOid = AlterTypeNamespace_oid(objid, nspOid, objsMoved);
            break;

        case OCLASS_COLLATION:
            oldNspOid = AlterCollationNamespace_oid(objid, nspOid);
            break;

        case OCLASS_CONVERSION:
            oldNspOid = AlterConversionNamespace_oid(objid, nspOid);
            break;

        case OCLASS_OPERATOR:
            oldNspOid = AlterOperatorNamespace_oid(objid, nspOid);
            break;

        case OCLASS_OPCLASS:
            oldNspOid = AlterOpClassNamespace_oid(objid, nspOid);
            break;

        case OCLASS_OPFAMILY:
            oldNspOid = AlterOpFamilyNamespace_oid(objid, nspOid);
            break;

        case OCLASS_TSPARSER:
            oldNspOid = AlterTSParserNamespace_oid(objid, nspOid);
            break;

        case OCLASS_TSDICT:
            oldNspOid = AlterTSDictionaryNamespace_oid(objid, nspOid);
            break;

        case OCLASS_TSTEMPLATE:
            oldNspOid = AlterTSTemplateNamespace_oid(objid, nspOid);
            break;

        case OCLASS_TSCONFIG:
            oldNspOid = AlterTSConfigurationNamespace_oid(objid, nspOid);
            break;

        default:
            break;
    }

    return oldNspOid;
}
/*                                                                                                                                                                                            
 * Generic function to change the namespace of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more
 * than change the namespace column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objid: OID of object to change the namespace of
 * nspOid: OID of new namespace
 *
 * Returns the OID of the object's previous namespace.
 */
 Oid
AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid)
{
    Oid         classId = RelationGetRelid(rel);
    int         oidCacheId = get_object_catcache_oid(classId);
    int         nameCacheId = get_object_catcache_name(classId);
    AttrNumber  Anum_name = get_object_attnum_name(classId);
    AttrNumber  Anum_namespace = get_object_attnum_namespace(classId);
    AttrNumber  Anum_owner = get_object_attnum_owner(classId);
    Oid         oldNspOid;
    Datum       name,
                obj_namespace;
    bool        isnull;
    HeapTuple   tup,
                newtup;
    Datum      *values;
    bool       *nulls;
    bool       *replaces;

    tup = SearchSysCacheCopy1(oidCacheId, ObjectIdGetDatum(objid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
             objid, RelationGetRelationName(rel));
   
    name = heap_getattr(tup, Anum_name, RelationGetDescr(rel), &isnull);
    Assert(!isnull);
    obj_namespace = (Datum)heap_getattr(tup, Anum_namespace, RelationGetDescr(rel),
                             &isnull);
    Assert(!isnull);
    oldNspOid = DatumGetObjectId(obj_namespace);

    /*
     * If the object is already in the correct namespace, we don't need to do
     * anything except fire the object access hook.
     */
    if (oldNspOid == nspOid) {
        return oldNspOid;
    }

    /* Check basic namespace related issues */
    CheckSetNamespace(oldNspOid, nspOid, classId, objid);

    /* Permission checks ... superusers can always do it */
    if (!superuser()) {
        Datum       owner; 
        Oid         ownerId;
        AclResult   aclresult;
    
        /* Fail if object does not have an explicit owner */
        if (Anum_owner <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     (errmsg("must be superuser to set schema of %s",
                             getObjectDescriptionOids(classId, objid)))));
    
        /* Otherwise, must be owner of the existing object */
        owner = heap_getattr(tup, Anum_owner, RelationGetDescr(rel), &isnull);
        Assert(!isnull);
        ownerId = DatumGetObjectId(owner);
        
        if (!has_privs_of_role(GetUserId(), ownerId))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPER,
                            NameStr(*(DatumGetName(name))));

        /* User must have CREATE privilege on new namespace */
        aclresult = pg_namespace_aclcheck(nspOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
                            get_namespace_name(nspOid));
    }
    
    /*
     * Check for duplicate name (more friendly than unique-index failure).
     * Since this is just a friendliness check, we can just skip it in cases
     * where there isn't suitable support.
     */
    if (classId == ProcedureRelationId) {
        Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);
        
        IsThereFunctionInNamespace(NameStr(proc->proname), proc->pronargs,
                                    &proc->proargtypes, nspOid);

        if (enable_plpgsql_gsdependency_guc()) {
            const char* old_func_format = format_procedure_no_visible(objid);
            const char* old_func_name = NameStr(proc->proname);
            bool is_null = false;
            Datum package_oid_datum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &is_null);
            Oid pkg_oid = DatumGetObjectId(package_oid_datum);
            if (gsplsql_exists_func_obj(oldNspOid, pkg_oid, old_func_format, old_func_name)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                        errmsg("The set schema operator of %s is not allowed, "
                               "because it is referenced by the other object.",
                            NameStr(proc->proname))));
            }
        }
    }
    else if (classId == CollationRelationId) {
        Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(tup);
    
        IsThereCollationInNamespace(NameStr(coll->collname), nspOid);
    }   
    else if (classId == OperatorClassRelationId) {   
        Form_pg_opclass opc = (Form_pg_opclass) GETSTRUCT(tup);
        
        IsThereOpClassInNamespace(NameStr(opc->opcname),
                                    opc->opcmethod, nspOid);
    }               
    else if (classId == OperatorFamilyRelationId) {                        
        Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(tup);
        
        IsThereOpFamilyInNamespace(NameStr(opf->opfname),
                                    opf->opfmethod, nspOid);
    }   
    else if (nameCacheId >= 0 &&
             SearchSysCacheExists2(nameCacheId, name,
                                    ObjectIdGetDatum(nspOid)))
        report_namespace_conflict(classId,
                                    NameStr(*(DatumGetName(name))),
                                    nspOid);
        
    /* Build modified tuple */
    values = (Datum*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
    nulls = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    replaces = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    values[Anum_namespace - 1] = ObjectIdGetDatum(nspOid);
    replaces[Anum_namespace - 1] = true;
    newtup = heap_modify_tuple(tup, RelationGetDescr(rel),
                               values, nulls, replaces);

    /* Perform actual update */
    CatalogTupleUpdate(rel, &tup->t_self, newtup);
    
    /* Release memory */
    pfree(values);
    pfree(nulls);
    pfree(replaces);
    
    /* update dependencies to point to the new schema */
    changeDependencyFor(classId, objid,
                        NamespaceRelationId, oldNspOid, nspOid);
    
    return oldNspOid;
} 

/*
 * Generic function to change the namespace of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more
 * than change the namespace column of a single catalog entry).
 *
 * The AlterFooNamespace() calls just above will call a function whose job
 * is to lookup the arguments for the generic function here.
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * oidCacheId: syscache that indexes this catalog by OID
 * nameCacheId: syscache that indexes this catalog by name and namespace
 *		(pass -1 if there is none)
 * objid: OID of object to change the namespace of
 * nspOid: OID of new namespace
 * Anum_name: column number of catalog's name column
 * Anum_namespace: column number of catalog's namespace column
 * Anum_owner: column number of catalog's owner column, or -1 if none
 * acl_kind: ACL type for object, or -1 if none assigned
 *
 * If the object does not have an owner or permissions, pass -1 for
 * Anum_owner and acl_kind.  In this case the calling user must be superuser.
 *
 * Returns the OID of the object's previous namespace.
 */
Oid AlterObjectNamespace(Relation rel, int oidCacheId, int nameCacheId, Oid objid, Oid nspOid, int Anum_name,
    int Anum_namespace, int Anum_owner, AclObjectKind acl_kind)
{
    Oid classId = RelationGetRelid(rel);
    Oid oldNspOid;
    Datum name, nmspace;
    bool isnull = false;
    HeapTuple tup, newtup;
    Datum* values = NULL;
    bool* nulls = NULL;
    bool* replaces = NULL;

    tup = SearchSysCacheCopy1(oidCacheId, ObjectIdGetDatum(objid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for object %u of catalog \"%s\"", objid, RelationGetRelationName(rel))));

    // AM_TODO: system table access, not necessary to use API
    name = tableam_tops_tuple_getattr(tup, Anum_name, RelationGetDescr(rel), &isnull);
    Assert(!isnull);
    nmspace = heap_getattr(tup, Anum_namespace, RelationGetDescr(rel), &isnull);
    Assert(!isnull);
    oldNspOid = DatumGetObjectId(nmspace);

    /* Check basic namespace related issues */
    CheckSetNamespace(oldNspOid, nspOid, classId, objid);

    /* Permission checks ... superusers can always do it */
    /* Database Security:  Support separation of privilege. */
    if (!isRelSuperuser()) {
        Datum owner;
        Oid ownerId;
        AclResult aclresult;

        /* Fail if object does not have an explicit owner */
        if (Anum_owner <= 0)
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("must be system admin to set schema of %s", getObjectDescriptionOids(classId, objid)))));

        /* Otherwise, must be owner of the existing object */
        owner = heap_getattr(tup, Anum_owner, RelationGetDescr(rel), &isnull);
        Assert(!isnull);
        ownerId = DatumGetObjectId(owner);
        if (!has_privs_of_role(GetUserId(), ownerId))
            aclcheck_error(ACLCHECK_NOT_OWNER, acl_kind, NameStr(*(DatumGetName(name))));

        /* User must have CREATE privilege on new namespace */
        aclresult = pg_namespace_aclcheck(nspOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(nspOid));
    }

    /*
     * Check for duplicate name (more friendly than unique-index failure).
     * Since this is just a friendliness check, we can just skip it in cases
     * where there isn't a suitable syscache available.
     */
    if (nameCacheId >= 0 && SearchSysCacheExists2(nameCacheId, name, ObjectIdGetDatum(nspOid)))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("%s already exists in schema \"%s\"",
                    getObjectDescriptionOids(classId, objid),
                    get_namespace_name(nspOid))));

    /* Build modified tuple */
    values = (Datum*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
    nulls = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    replaces = (bool*)palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    values[Anum_namespace - 1] = ObjectIdGetDatum(nspOid);
    replaces[Anum_namespace - 1] = true;
    newtup = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    /* Perform actual update */
    simple_heap_update(rel, &tup->t_self, newtup);
    CatalogUpdateIndexes(rel, newtup);

    /* Release memory */
    pfree_ext(values);
    pfree_ext(nulls);
    pfree_ext(replaces);

    /* update dependencies to point to the new schema */
    (void)changeDependencyFor(classId, objid, NamespaceRelationId, oldNspOid, nspOid);

    return oldNspOid;
}

/*
 * Executes an ALTER OBJECT / OWNER TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
ObjectAddress ExecAlterOwnerStmt(AlterOwnerStmt* stmt)
{
    const char* newOwnerName = stmt->newowner;
    Oid newowner;
    if (strcmp(newOwnerName, "current_user") == 0) {
        /* CURRENT_USER */
        newowner = GetUserId();
    } else if (strcmp(newOwnerName, "session_user") == 0) {
        /* SESSION_USER */
        newowner = GetSessionUserId();
    } else {
        /* Normal User */
        newowner = get_role_oid(newOwnerName, false);
    }

    switch (stmt->objectType) {
        case OBJECT_AGGREGATE:
            /* Given ordered set aggregate with no direct args, aggr_args variable is modified in gram.y.
               So the parse of aggr_args should be changed. See gram.y for detail. */
            stmt->objarg = (List*)linitial(stmt->objarg);
            return AlterAggregateOwner(stmt->object, stmt->objarg, newowner);

        case OBJECT_COLLATION:
            return AlterCollationOwner(stmt->object, newowner);

        case OBJECT_CONVERSION:
            return AlterConversionOwner(stmt->object, newowner);

        case OBJECT_DATABASE:
            return AlterDatabaseOwner(strVal(linitial(stmt->object)), newowner);

       case OBJECT_EVENT_TRIGGER:
            return AlterEventTriggerOwner(strVal(linitial(stmt->object)), newowner);
            
        case OBJECT_FUNCTION:
            return AlterFunctionOwner(stmt->object, stmt->objarg, newowner);

        case OBJECT_PACKAGE:
            return AlterPackageOwner(stmt->object, newowner);
            break;
        case OBJECT_LANGUAGE:
            return AlterLanguageOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_LARGEOBJECT:
            return LargeObjectAlterOwner(oidparse((Node*)linitial(stmt->object)), newowner);

        case OBJECT_OPERATOR:
            Assert(list_length(stmt->objarg) == 2);
            return AlterOperatorOwner(
                stmt->object, (TypeName*)linitial(stmt->objarg), (TypeName*)lsecond(stmt->objarg), newowner);

        case OBJECT_OPCLASS:
        {
            List* object_names;
            object_names=list_copy(stmt->object);
            object_names=list_delete_first(object_names);
            ObjectAddress obj_opclass=AlterOpClassOwner(object_names, ((Value*)linitial(stmt->object))->val.str, newowner);
            list_free_ext(object_names);
            return obj_opclass;
        }

        case OBJECT_OPFAMILY:
        {
            List* object_names;
            object_names=list_copy(stmt->object);
            object_names=list_delete_first(object_names);
            ObjectAddress obj_opfamily=AlterOpFamilyOwner(object_names, ((Value*)linitial(stmt->object))->val.str, newowner);
            list_free_ext(object_names);
            return obj_opfamily;
        }
        case OBJECT_SCHEMA:
            return AlterSchemaOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_TABLESPACE:
            return AlterTableSpaceOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_TYPE:
        case OBJECT_DOMAIN: /* same as TYPE */
            return AlterTypeOwner(stmt->object, newowner, stmt->objectType, true);

        case OBJECT_TSDICTIONARY:
            return AlterTSDictionaryOwner(stmt->object, newowner);

        case OBJECT_TSCONFIGURATION:
            return AlterTSConfigurationOwner(stmt->object, newowner);

        case OBJECT_FDW:
            return AlterForeignDataWrapperOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_FOREIGN_SERVER:
            return AlterForeignServerOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_DATA_SOURCE:
            return AlterDataSourceOwner(strVal(linitial(stmt->object)), newowner);
        case OBJECT_DIRECTORY:
            return AlterDirectoryOwner(strVal(linitial(stmt->object)), newowner);

        case OBJECT_SYNONYM:
            return AlterSynonymOwner(stmt->object, newowner);

        case OBJECT_PUBLICATION:
            return AlterPublicationOwner(strVal(linitial(stmt->object)), newowner);
            break;

        case OBJECT_SUBSCRIPTION:
            return AlterSubscriptionOwner(strVal(linitial(stmt->object)), newowner);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized AlterOwnerStmt type: %d", (int)stmt->objectType)));
            return InvalidObjectAddress;
    }
}
