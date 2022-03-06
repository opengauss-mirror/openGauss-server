/* -------------------------------------------------------------------------
 *
 * objectaddress.cpp
 *	  functions for working with ObjectAddresses
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/objectaddress.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/gs_model.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/gs_package.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_extension_data_source.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/directory.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "foreign/foreign.h"
#include "libpq/auth.h"
#include "libpq/be-fsstubs.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteSupport.h"
#include "storage/lmgr.h"
#include "storage/sinval.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "datasource/datasource.h"

/*
 * ObjectProperty
 *
 * This array provides a common part of system object structure; to help
 * consolidate routines to handle various kind of object classes.
 */
typedef struct {
    Oid class_oid;               /* oid of catalog */
    Oid oid_index_oid;           /* oid of index on system oid column */
    int oid_catcache_id;         /* id of catcache on system oid column	*/
    AttrNumber attnum_namespace; /* attnum of namespace field */
} ObjectPropertyType;

static THR_LOCAL const ObjectPropertyType ObjectProperty[] = {{CastRelationId, CastOidIndexId, -1, InvalidAttrNumber},
    {CollationRelationId, CollationOidIndexId, COLLOID, Anum_pg_collation_collnamespace},
    {ConstraintRelationId, ConstraintOidIndexId, CONSTROID, Anum_pg_constraint_connamespace},
    {ConversionRelationId, ConversionOidIndexId, CONVOID, Anum_pg_conversion_connamespace},
    {DatabaseRelationId, DatabaseOidIndexId, DATABASEOID, InvalidAttrNumber},
    {DataSourceRelationId, DataSourceOidIndexId, DATASOURCEOID, InvalidAttrNumber},
    {
        ExtensionRelationId, ExtensionOidIndexId, -1, InvalidAttrNumber /* extension doesn't belong to extnamespace */
    },
    {ForeignDataWrapperRelationId, ForeignDataWrapperOidIndexId, FOREIGNDATAWRAPPEROID, InvalidAttrNumber},
    {ForeignServerRelationId, ForeignServerOidIndexId, FOREIGNSERVEROID, InvalidAttrNumber},
    {ProcedureRelationId, ProcedureOidIndexId, PROCOID, Anum_pg_proc_pronamespace},
    {PackageRelationId, PackageOidIndexId, PACKAGEOID, Anum_gs_package_pkgnamespace},
    {
        LanguageRelationId,
        LanguageOidIndexId,
        LANGOID,
        InvalidAttrNumber,
    },
    {LargeObjectMetadataRelationId, LargeObjectMetadataOidIndexId, -1, InvalidAttrNumber},
    {
        OperatorClassRelationId,
        OpclassOidIndexId,
        CLAOID,
        Anum_pg_opclass_opcnamespace,
    },
    {ModelRelationId, GsModelOidIndexId, DB4AI_MODEL, InvalidAttrNumber},
    {OperatorRelationId, OperatorOidIndexId, OPEROID, Anum_pg_operator_oprnamespace},
    {OperatorFamilyRelationId, OpfamilyOidIndexId, OPFAMILYOID, Anum_pg_opfamily_opfnamespace},
    {AuthIdRelationId, AuthIdOidIndexId, AUTHOID, InvalidAttrNumber},
    {RewriteRelationId, RewriteOidIndexId, -1, InvalidAttrNumber},
    {NamespaceRelationId, NamespaceOidIndexId, NAMESPACEOID, InvalidAttrNumber},
    {RelationRelationId, ClassOidIndexId, RELOID, Anum_pg_class_relnamespace},
    {TableSpaceRelationId, TablespaceOidIndexId, TABLESPACEOID, InvalidAttrNumber},
    {TriggerRelationId, TriggerOidIndexId, -1, InvalidAttrNumber},
    {TSConfigRelationId, TSConfigOidIndexId, TSCONFIGOID, Anum_pg_ts_config_cfgnamespace},
    {TSDictionaryRelationId, TSDictionaryOidIndexId, TSDICTOID, Anum_pg_ts_dict_dictnamespace},
    {TSParserRelationId, TSParserOidIndexId, TSPARSEROID, Anum_pg_ts_parser_prsnamespace},
    {
        TSTemplateRelationId,
        TSTemplateOidIndexId,
        TSTEMPLATEOID,
        Anum_pg_ts_template_tmplnamespace,
    },
    {TypeRelationId, TypeOidIndexId, TYPEOID, Anum_pg_type_typnamespace},
    {RlsPolicyRelationId, PgRlspolicyOidIndex, -1, InvalidAttrNumber},
    {
        PublicationRelationId,
        PublicationObjectIndexId,
        PUBLICATIONOID,
        InvalidAttrNumber
    },
    {
        SubscriptionRelationId,
        SubscriptionObjectIndexId,
        SUBSCRIPTIONOID,
        InvalidAttrNumber
    }};

static ObjectAddress get_object_address_unqualified(ObjectType objtype, List* qualname, bool missing_ok);
static ObjectAddress get_relation_by_qualified_name(
    ObjectType objtype, List* objname, Relation* relp, LOCKMODE lockmode, bool missing_ok);
static ObjectAddress get_object_address_relobject(ObjectType objtype, List* objname, Relation* relp, bool missing_ok);
static ObjectAddress get_object_address_attribute(
    ObjectType objtype, List* objname, Relation* relp, LOCKMODE lockmode, bool missing_ok);
static ObjectAddress get_object_address_type(ObjectType objtype, List* objname, bool missing_ok);
static ObjectAddress get_object_address_opcf(ObjectType objtype, List* objname, List* objargs, bool missing_ok);
static const ObjectPropertyType* get_object_property_data(Oid class_id);

/*
 * Translate an object name and arguments (as passed by the parser) to an
 * ObjectAddress.
 *
 * The returned object will be locked using the specified lockmode.  If a
 * sub-object is looked up, the parent object will be locked instead.
 *
 * If the object is a relation or a child object of a relation (e.g. an
 * attribute or contraint), the relation is also opened and *relp receives
 * the open relcache entry pointer; otherwise, *relp is set to NULL.  This
 * is a bit grotty but it makes life simpler, since the caller will
 * typically need the relcache entry too.  Caller must close the relcache
 * entry when done with it.  The relation is locked with the specified lockmode
 * if the target object is the relation itself or an attribute, but for other
 * child objects, only AccessShareLock is acquired on the relation.
 *
 * We don't currently provide a function to release the locks acquired here;
 * typically, the lock must be held until commit to guard against a concurrent
 * drop operation.
 */
ObjectAddress get_object_address(
    ObjectType objtype, List* objname, List* objargs, Relation* relp, LOCKMODE lockmode, bool missing_ok)
{
    ObjectAddress address;
    ObjectAddress old_address = {InvalidOid, InvalidOid, 0};
    Relation relation = NULL;
    Relation old_relation = NULL;
    List* objargs_agg = NULL;

    /* Some kind of lock must be taken. */
    Assert(lockmode != NoLock);
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        /*
         * Remember this value, so that, after looking up the object name and
         * locking it, we can check whether any invalidation messages have
         * been processed that might require a do-over.
         */
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }

        /* Look up object address. */
        switch (objtype) {
            case OBJECT_INDEX:
            case OBJECT_SEQUENCE:
            case OBJECT_LARGE_SEQUENCE:
            case OBJECT_TABLE:
            case OBJECT_VIEW:
            case OBJECT_CONTQUERY:
            case OBJECT_MATVIEW:
            case OBJECT_FOREIGN_TABLE:
            case OBJECT_STREAM:
                address = get_relation_by_qualified_name(objtype, objname, &relation, lockmode, missing_ok);
                break;
            case OBJECT_COLUMN:
                address = get_object_address_attribute(objtype, objname, &relation, lockmode, missing_ok);
                break;
            case OBJECT_RULE:
            case OBJECT_TRIGGER:
            case OBJECT_CONSTRAINT:
            case OBJECT_RLSPOLICY:
                address = get_object_address_relobject(objtype, objname, &relation, missing_ok);
                break;
            case OBJECT_DATABASE:
            case OBJECT_DB4AI_MODEL:
            case OBJECT_EXTENSION:
            case OBJECT_TABLESPACE:
            case OBJECT_ROLE:
            case OBJECT_USER:
            case OBJECT_SCHEMA:
            case OBJECT_LANGUAGE:
            case OBJECT_FDW:
            case OBJECT_FOREIGN_SERVER:
            case OBJECT_DATA_SOURCE:
            case OBJECT_PUBLICATION:
            case OBJECT_SUBSCRIPTION:
                address = get_object_address_unqualified(objtype, objname, missing_ok);
                break;
            case OBJECT_TYPE:
            case OBJECT_DOMAIN:
                address = get_object_address_type(objtype, objname, missing_ok);
                break;
            case OBJECT_AGGREGATE:
                /* Given ordered set aggregate with no direct args, aggr_args variable is modified in gram.y.
                   So the parse of aggr_args should be changed. See gram.y for detail. */
                objargs_agg = (List*)linitial(objargs);

                address.classId = ProcedureRelationId;
                address.objectId = LookupAggNameTypeNames(objname, objargs_agg, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_FUNCTION:
                address.classId = ProcedureRelationId;
                address.objectId = LookupFuncNameOptTypeNames(objname, objargs, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_PACKAGE:
                address.classId = PackageRelationId;
                address.objectId = PackageNameListGetOid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_PACKAGE_BODY:
                address.classId = PackageRelationId;
                address.objectId = PackageNameListGetOid(objname, missing_ok);
                address.objectSubId = (int32)address.objectId; /* same as objectId for package body */
                break;
            case OBJECT_OPERATOR:
                Assert(list_length(objargs) == 2);
                address.classId = OperatorRelationId;
                address.objectId = LookupOperNameTypeNames(
                    NULL, objname, (TypeName*)linitial(objargs), (TypeName*)lsecond(objargs), missing_ok, -1);
                address.objectSubId = 0;
                break;
            case OBJECT_COLLATION:
                address.classId = CollationRelationId;
                address.objectId = get_collation_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_CONVERSION:
                address.classId = ConversionRelationId;
                address.objectId = get_conversion_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_OPCLASS:
            case OBJECT_OPFAMILY:
                address = get_object_address_opcf(objtype, objname, objargs, missing_ok);
                break;
            case OBJECT_LARGEOBJECT:
                Assert(list_length(objname) == 1);
                address.classId = LargeObjectRelationId;
                address.objectId = oidparse((Node*)linitial(objname));
                address.objectSubId = 0;
                if (!LargeObjectExists(address.objectId)) {
                    if (!missing_ok)
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("large object %u does not exist", address.objectId)));
                }
                break;
            case OBJECT_CAST: {
                TypeName* sourcetype = (TypeName*)linitial(objname);
                TypeName* targettype = (TypeName*)linitial(objargs);
                Oid sourcetypeid = typenameTypeId(NULL, sourcetype);
                Oid targettypeid = typenameTypeId(NULL, targettype);

                address.classId = CastRelationId;
                address.objectId = get_cast_oid(sourcetypeid, targettypeid, missing_ok);
                address.objectSubId = 0;
            } break;
            case OBJECT_TSPARSER:
                address.classId = TSParserRelationId;
                address.objectId = get_ts_parser_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_TSDICTIONARY:
                address.classId = TSDictionaryRelationId;
                address.objectId = get_ts_dict_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_TSTEMPLATE:
                address.classId = TSTemplateRelationId;
                address.objectId = get_ts_template_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_TSCONFIGURATION:
                address.classId = TSConfigRelationId;
                address.objectId = get_ts_config_oid(objname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_DIRECTORY:
                address = get_object_address_unqualified(objtype, objname, missing_ok);
                break;
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
                /* placate compiler, in case it thinks elog might return */
                address.classId = InvalidOid;
                address.objectId = InvalidOid;
                address.objectSubId = 0;
        }

        /*
         * If we could not find the supplied object, return without locking.
         */
        if (!OidIsValid(address.objectId)) {
            Assert(missing_ok);
            return address;
        }

        /*
         * If we're retrying, see if we got the same answer as last time.  If
         * so, we're done; if not, we locked the wrong thing, so give up our
         * lock.
         */
        if (OidIsValid(old_address.classId)) {
            if (old_address.classId == address.classId && old_address.objectId == address.objectId &&
                old_address.objectSubId == address.objectSubId) {
                    if (old_relation != NULL) {
                        Assert(old_relation == relation);
                        /* should be 2 */
                        Assert(old_relation->rd_refcnt > 1);
                        heap_close(old_relation, NoLock);
                        old_relation = NULL;
                    }
                    break;
                }
            if (old_address.classId != RelationRelationId) {
                if (old_relation != NULL) {
                    heap_close(old_relation, NoLock);
                    old_relation = NULL;
                }
                if (IsSharedRelation(old_address.classId))
                    UnlockSharedObject(old_address.classId, old_address.objectId, 0, lockmode);
                else
                    UnlockDatabaseObject(old_address.classId, old_address.objectId, 0, lockmode);
            }
        }

        /*
         * If we're dealing with a relation or attribute, then the relation is
         * already locked.	Otherwise, we lock it now.
         */
        if (address.classId != RelationRelationId) {
            if (IsSharedRelation(address.classId))
                LockSharedObject(address.classId, address.objectId, 0, lockmode);
            else
                LockDatabaseObject(address.classId, address.objectId, 0, lockmode);
        }

        /*
         * At this point, we've resolved the name to an OID and locked the
         * corresponding database object.  However, it's possible that by the
         * time we acquire the lock on the object, concurrent DDL has modified
         * the database in such a way that the name we originally looked up no
         * longer resolves to that OID.
         *
         * We can be certain that this isn't an issue if (a) no shared
         * invalidation messages have been processed or (b) we've locked a
         * relation somewhere along the line.  All the relation name lookups
         * in this module ultimately use RangeVarGetRelid() to acquire a
         * relation lock, and that function protects against the same kinds of
         * races we're worried about here.  Even when operating on a
         * constraint, rule, or trigger, we still acquire AccessShareLock on
         * the relation, which is enough to freeze out any concurrent DDL.
         *
         * In all other cases, however, it's possible that the name we looked
         * up no longer refers to the object we locked, so we retry the lookup
         * and see whether we get the same answer.
         */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }
        old_address = address;
        old_relation = relation;
    }

    /* Return the object address and the relation. */
    *relp = relation;
    return address;
}

/*
 * Find an ObjectAddress for a type of object that is identified by an
 * unqualified name.
 */
static ObjectAddress get_object_address_unqualified(ObjectType objtype, List* qualname, bool missing_ok)
{
    const char* name = NULL;
    ObjectAddress address;

    /*
     * The types of names handled by this function are not permitted to be
     * schema-qualified or catalog-qualified.
     */
    if (list_length(qualname) != 1) {
        const char* msg = NULL;

        switch (objtype) {
            case OBJECT_DATABASE:
                msg = gettext_noop("database name cannot be qualified");
                break;
            case OBJECT_DB4AI_MODEL:
                msg = gettext_noop("model name cannot be qualified");
                break;
            case OBJECT_EXTENSION:
                msg = gettext_noop("extension name cannot be qualified");
                break;
            case OBJECT_TABLESPACE:
                msg = gettext_noop("tablespace name cannot be qualified");
                break;
            case OBJECT_ROLE:
            case OBJECT_USER:
                msg = gettext_noop("role name cannot be qualified");
                break;
            case OBJECT_SCHEMA:
                msg = gettext_noop("schema name cannot be qualified");
                break;
            case OBJECT_LANGUAGE:
                msg = gettext_noop("language name cannot be qualified");
                break;
            case OBJECT_FDW:
                msg = gettext_noop("foreign-data wrapper name cannot be qualified");
                break;
            case OBJECT_FOREIGN_SERVER:
                msg = gettext_noop("server name cannot be qualified");
                break;
            case OBJECT_DATA_SOURCE:
                msg = gettext_noop("data source name cannot be qualified");
                break;
            case OBJECT_DIRECTORY:
                msg = gettext_noop("directory name cannot be qualified");
                break;
            case OBJECT_PUBLICATION:
                msg = gettext_noop("publication name cannot be qualified");
                break;
            case OBJECT_SUBSCRIPTION:
                msg = gettext_noop("subscription name cannot be qualified");
                break;
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
                msg = NULL; /* placate compiler */
        }
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s", _(msg))));
    }

    /* Format is valid, extract the actual name. */
    name = strVal(linitial(qualname));

    /* Translate name to OID. */
    switch (objtype) {
        case OBJECT_DATABASE:
            address.classId = DatabaseRelationId;
            address.objectId = get_database_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_DB4AI_MODEL:
            address.classId = ModelRelationId;
            address.objectId = get_model_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_EXTENSION:
            address.classId = ExtensionRelationId;
            address.objectId = get_extension_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_TABLESPACE:
            address.classId = TableSpaceRelationId;
            address.objectId = get_tablespace_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_ROLE:
        case OBJECT_USER:
            address.classId = AuthIdRelationId;
            address.objectId = get_role_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_SCHEMA:
            address.classId = NamespaceRelationId;
            address.objectId = get_namespace_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_LANGUAGE:
            address.classId = LanguageRelationId;
            address.objectId = get_language_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_FDW:
            address.classId = ForeignDataWrapperRelationId;
            address.objectId = get_foreign_data_wrapper_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_FOREIGN_SERVER:
            address.classId = ForeignServerRelationId;
            address.objectId = get_foreign_server_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_DATA_SOURCE:
            address.classId = DataSourceRelationId;
            address.objectId = get_data_source_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_DIRECTORY:
            address.classId = PgDirectoryRelationId;
            address.objectId = get_directory_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_PUBLICATION:
            address.classId = PublicationRelationId;
            address.objectId = get_publication_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_SUBSCRIPTION:
            address.classId = SubscriptionRelationId;
            address.objectId = get_subscription_oid(name, missing_ok);
            address.objectSubId = 0;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
            /* placate compiler, which doesn't know elog won't return */
            address.classId = InvalidOid;
            address.objectId = InvalidOid;
            address.objectSubId = 0;
    }

    return address;
}

/*
 * Locate a relation by qualified name.
 */
static ObjectAddress get_relation_by_qualified_name(
    ObjectType objtype, List* objname, Relation* relp, LOCKMODE lockmode, bool missing_ok)
{
    Relation relation;
    ObjectAddress address;

    address.classId = RelationRelationId;
    address.objectId = InvalidOid;
    address.objectSubId = 0;

    relation = relation_openrv_extended(makeRangeVarFromNameList(objname), lockmode, missing_ok);
    if (!relation)
        return address;

    switch (objtype) {
        case OBJECT_INDEX:
            if (relation->rd_rel->relkind != RELKIND_INDEX &&
                relation->rd_rel->relkind != RELKIND_GLOBAL_INDEX)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not an index", RelationGetRelationName(relation))));
            break;
        case OBJECT_SEQUENCE:
            if (relation->rd_rel->relkind != RELKIND_SEQUENCE)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a sequence", RelationGetRelationName(relation))));
            break;
        case OBJECT_LARGE_SEQUENCE:
            if (relation->rd_rel->relkind != RELKIND_LARGE_SEQUENCE)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a large sequence", RelationGetRelationName(relation))));
            break;
        case OBJECT_TABLE:
            if (relation->rd_rel->relkind != RELKIND_RELATION)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a table", RelationGetRelationName(relation))));
            break;
        case OBJECT_VIEW:
            if (relation->rd_rel->relkind != RELKIND_VIEW)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a view", RelationGetRelationName(relation))));
            break;
        case OBJECT_CONTQUERY:
            if (relation->rd_rel->relkind != RELKIND_CONTQUERY)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a contview", RelationGetRelationName(relation))));
            break;
        case OBJECT_MATVIEW:
            if (relation->rd_rel->relkind != RELKIND_MATVIEW)
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("\"%s\" is not a materialized view",
                                RelationGetRelationName(relation))));
            break;
        case OBJECT_FOREIGN_TABLE:
            if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a foreign table", RelationGetRelationName(relation))));
            break;
        case OBJECT_STREAM:
            if (relation->rd_rel->relkind != RELKIND_STREAM)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is not a stream", RelationGetRelationName(relation))));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
            break;
    }

    /* Done. */
    address.objectId = RelationGetRelid(relation);
    *relp = relation;

    return address;
}

/*
 * Find object address for an object that is attached to a relation.
 *
 * Note that we take only an AccessShareLock on the relation.  We need not
 * pass down the LOCKMODE from get_object_address(), because that is the lock
 * mode for the object itself, not the relation to which it is attached.
 */
static ObjectAddress get_object_address_relobject(ObjectType objtype, List* objname, Relation* relp, bool missing_ok)
{
    ObjectAddress address;
    Relation relation = NULL;
    int nnames;
    const char* depname = NULL;

    /* Extract name of dependent object. */
    depname = strVal(lfirst(list_tail(objname)));

    /* Separate relation name from dependent object name. */
    nnames = list_length(objname);
    if (nnames < 2) {
        Oid reloid;

        /*
         * For compatibility with very old releases, we sometimes allow users
         * to attempt to specify a rule without mentioning the relation name.
         * If there's only rule by that name in the entire database, this will
         * work.  But objects other than rules don't get this special
         * treatment.
         */
        if (objtype != OBJECT_RULE)
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("must specify relation and object name")));
        address.classId = RewriteRelationId;
        address.objectId = get_rewrite_oid_without_relid(depname, &reloid, missing_ok);
        address.objectSubId = 0;

        /*
         * Caller is expecting to get back the relation, even though we didn't
         * end up using it to find the rule.
         */
        if (OidIsValid(address.objectId))
            relation = heap_open(reloid, AccessShareLock);
    } else {
        List* relname = NIL;
        Oid reloid;

        /* Extract relation name and open relation, here allow it is synonym object. */
        relname = list_truncate(list_copy(objname), nnames - 1);
        relation = HeapOpenrvExtended(makeRangeVarFromNameList(relname), AccessShareLock, false, true);
        reloid = RelationGetRelid(relation);

        switch (objtype) {
            case OBJECT_RULE:
                address.classId = RewriteRelationId;
                address.objectId = get_rewrite_oid(reloid, depname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_TRIGGER:
                address.classId = TriggerRelationId;
                address.objectId = get_trigger_oid(reloid, depname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_CONSTRAINT:
                address.classId = ConstraintRelationId;
                address.objectId = get_relation_constraint_oid(reloid, depname, missing_ok);
                address.objectSubId = 0;
                break;
            case OBJECT_RLSPOLICY:
                address.classId = RlsPolicyRelationId;
                address.objectId = relation ? get_rlspolicy_oid(reloid, depname, missing_ok) : InvalidOid;
                address.objectSubId = 0;
                break;
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
                /* placate compiler, which doesn't know elog won't return */
                address.classId = InvalidOid;
                address.objectId = InvalidOid;
                address.objectSubId = 0;
        }

        /* Avoid relcache leak when object not found. */
        if (!OidIsValid(address.objectId)) {
            heap_close(relation, AccessShareLock);
            relation = NULL; /* department of accident prevention */
            return address;
        }
    }

    /* Done. */
    *relp = relation;
    return address;
}

/*
 * Find the ObjectAddress for an attribute.
 */
static ObjectAddress get_object_address_attribute(
    ObjectType objtype, List* objname, Relation* relp, LOCKMODE lockmode, bool missing_ok)
{
    ObjectAddress address;
    List* relname = NIL;
    Oid reloid;
    Relation relation;
    const char* attname = NULL;
    AttrNumber attnum;

    /* Extract relation name and open relation. */
    if (list_length(objname) < 2)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("column name must be qualified")));
    attname = strVal(lfirst(list_tail(objname)));
    relname = list_truncate(list_copy(objname), list_length(objname) - 1);
    relation = relation_openrv_extended(makeRangeVarFromNameList(relname), lockmode, false, true);
    reloid = RelationGetRelid(relation);

    /* Look up attribute and construct return value. */
    attnum = get_attnum(reloid, attname);
    if (attnum == InvalidAttrNumber) {
        if (!missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" of relation \"%s\" does not exist", attname, NameListToString(relname))));

        /* close but save lock */
        heap_close(relation, NoLock);
        address.classId = RelationRelationId;
        address.objectId = InvalidOid;
        address.objectSubId = InvalidAttrNumber;
        return address;
    }

    address.classId = RelationRelationId;
    address.objectId = reloid;
    address.objectSubId = attnum;

    *relp = relation;
    return address;
}

/*
 * Find the ObjectAddress for a type or domain
 */
static ObjectAddress get_object_address_type(ObjectType objtype, List* objname, bool missing_ok)
{
    ObjectAddress address;
    TypeName* typname = NULL;
    Type tup;

    typname = makeTypeNameFromNameList(objname);

    address.classId = TypeRelationId;
    address.objectId = InvalidOid;
    address.objectSubId = 0;

    tup = LookupTypeName(NULL, typname, NULL);
    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type \"%s\" does not exist", TypeNameToString(typname))));
        return address;
    }
    address.objectId = typeTypeId(tup);

    if (objtype == OBJECT_DOMAIN) {
        if (((Form_pg_type)GETSTRUCT(tup))->typtype != TYPTYPE_DOMAIN)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a domain", TypeNameToString(typname))));
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (IsPackageDependType(typeTypeId(tup), InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("Not allowed to drop type \"%s\"", TypeNameToString(typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif

    ReleaseSysCache(tup);

    return address;
}

/*
 * Find the ObjectAddress for an opclass or opfamily.
 */
static ObjectAddress get_object_address_opcf(ObjectType objtype, List* objname, List* objargs, bool missing_ok)
{
    Oid amoid;
    ObjectAddress address;

    Assert(list_length(objargs) == 1);
    amoid = get_am_oid(strVal(linitial(objargs)), false);

    switch (objtype) {
        case OBJECT_OPCLASS:
            address.classId = OperatorClassRelationId;
            address.objectId = get_opclass_oid(amoid, objname, missing_ok);
            address.objectSubId = 0;
            break;
        case OBJECT_OPFAMILY:
            address.classId = OperatorFamilyRelationId;
            address.objectId = get_opfamily_oid(amoid, objname, missing_ok);
            address.objectSubId = 0;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
            /* placate compiler, which doesn't know elog won't return */
            address.classId = InvalidOid;
            address.objectId = InvalidOid;
            address.objectSubId = 0;
    }

    return address;
}

/*
 * Check ownership of an object previously identified by get_object_address.
 */
void check_object_ownership(
    Oid roleid, ObjectType objtype, ObjectAddress address, List* objname, List* objargs, Relation relation)
{
    switch (objtype) {
        case OBJECT_INDEX:
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_TABLE:
        case OBJECT_VIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_MATVIEW:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_COLUMN:
            if (!pg_class_ownercheck(RelationGetRelid(relation), roleid)) {
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, RelationGetRelationName(relation));
            }
            break;
        case OBJECT_RULE:
        case OBJECT_TRIGGER:
        case OBJECT_CONSTRAINT:
        case OBJECT_RLSPOLICY:
            if (!pg_class_ownercheck(RelationGetRelid(relation), roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, RelationGetRelationName(relation));
            break;
        case OBJECT_DATABASE:
            if (!pg_database_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DATABASE, NameListToString(objname));
            break;
        case OBJECT_TYPE:
            if (!pg_type_ownercheck(address.objectId, roleid))
                aclcheck_error_type(ACLCHECK_NO_PRIV, address.objectId);
            break;
        case OBJECT_DB4AI_MODEL:
        case OBJECT_DOMAIN:
        case OBJECT_ATTRIBUTE:
            if (!pg_type_ownercheck(address.objectId, roleid))
                aclcheck_error_type(ACLCHECK_NOT_OWNER, address.objectId);
            break;
        case OBJECT_AGGREGATE:
        case OBJECT_FUNCTION:
            if (!pg_proc_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, NameListToString(objname));
            break;
        case OBJECT_PACKAGE:
        case OBJECT_PACKAGE_BODY:
            if (!pg_package_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PACKAGE, NameListToString(objname));
            break;
        case OBJECT_OPERATOR:
            if (!pg_oper_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPER, NameListToString(objname));
            break;
        case OBJECT_SCHEMA:
            if (!pg_namespace_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_NAMESPACE, NameListToString(objname));
            break;
        case OBJECT_COLLATION:
            if (!pg_collation_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_COLLATION, NameListToString(objname));
            break;
        case OBJECT_CONVERSION:
            if (!pg_conversion_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONVERSION, NameListToString(objname));
            break;
        case OBJECT_EXTENSION:
            if (!pg_extension_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EXTENSION, NameListToString(objname));
            break;
        case OBJECT_FDW:
            if (!pg_foreign_data_wrapper_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FDW, NameListToString(objname));
            break;
        case OBJECT_FOREIGN_SERVER:
            if (!pg_foreign_server_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, NameListToString(objname));
            break;
        case OBJECT_LANGUAGE:
            if (!pg_language_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_LANGUAGE, NameListToString(objname));
            break;
        case OBJECT_OPCLASS:
            if (!pg_opclass_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS, NameListToString(objname));
            break;
        case OBJECT_OPFAMILY:
            if (!pg_opfamily_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPFAMILY, NameListToString(objname));
            break;
        case OBJECT_LARGEOBJECT:
            if (!u_sess->attr.attr_sql.lo_compat_privileges && !pg_largeobject_ownercheck(address.objectId, roleid))
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be owner of large object %u", address.objectId)));
            break;
        case OBJECT_CAST: {
            /* We can only check permissions on the source/target types */
            TypeName* sourcetype = (TypeName*)linitial(objname);
            TypeName* targettype = (TypeName*)linitial(objargs);
            Oid sourcetypeid = typenameTypeId(NULL, sourcetype);
            Oid targettypeid = typenameTypeId(NULL, targettype);

            if (!pg_type_ownercheck(sourcetypeid, roleid) && !pg_type_ownercheck(targettypeid, roleid))
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be owner of type %s or type %s",
                            format_type_be(sourcetypeid),
                            format_type_be(targettypeid))));
        } break;
        case OBJECT_TABLESPACE:
            if (!pg_tablespace_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_TABLESPACE, NameListToString(objname));
            break;
        case OBJECT_TSDICTIONARY:
            if (!pg_ts_dict_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSDICTIONARY, NameListToString(objname));
            break;
        case OBJECT_TSCONFIGURATION:
            if (!pg_ts_config_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TSCONFIGURATION, NameListToString(objname));
            break;
        case OBJECT_ROLE:
        case OBJECT_USER:
            /*
             * We treat roles as being "owned" by those with CREATEROLE priv,
             * except that superusers are only owned by superusers and
             * opradmin user and persistence user are only owned by the initial user.
             */
            if (is_role_persistence(address.objectId)) {
                if(roleid != INITIAL_USER_ID)
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be initial user")));
            } else if (isOperatoradmin(address.objectId)) {
                if(roleid != INITIAL_USER_ID)
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be initial user")));
            } else if (superuser_arg(address.objectId)) {
                if (!superuser_arg(roleid))
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin")));
            }
            /* Database Security:  Support separation of privilege.*/
            else if (systemDBA_arg(address.objectId)) {
                if (!superuser_arg(roleid))
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin")));
            } else {
                if (!has_createrole_privilege(roleid))
                    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must have CREATEROLE privilege")));
            }
            break;
        case OBJECT_TSPARSER:
        case OBJECT_TSTEMPLATE:
            /* Database Security:  Support separation of privilege.*/
            /* We treat these object types as being owned by superusers */
            if (!(superuser_arg(roleid) || systemDBA_arg(roleid)))
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin")));
            break;

        case OBJECT_DATA_SOURCE:
            if (!pg_extension_data_source_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATA_SOURCE, NameListToString(objname));
            break;
        case OBJECT_DIRECTORY:
            if (!pg_directory_ownercheck(address.objectId, roleid))
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DIRECTORY, NameListToString(objname));
            break;
        case OBJECT_PUBLICATION:
            if (!pg_publication_ownercheck(address.objectId, roleid)) {
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PUBLICATION, NameListToString(objname));
            }
            break;
        case OBJECT_SUBSCRIPTION:
            if (!pg_subscription_ownercheck(address.objectId, roleid)) {
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, NameListToString(objname));
            }
            break;
        default:
            ereport(
                ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object type: %d", (int)objtype)));
    }
}

/*
 * get_object_namespace
 *
 * Find the schema containing the specified object.  For non-schema objects,
 * this function returns InvalidOid.
 */
Oid get_object_namespace(const ObjectAddress* address)
{
    int cache;
    HeapTuple tuple;
    bool isnull = false;
    Oid oid;
    const ObjectPropertyType* property = NULL;

    /* If not owned by a namespace, just return InvalidOid. */
    property = get_object_property_data(address->classId);
    if (property->attnum_namespace == InvalidAttrNumber)
        return InvalidOid;

    /* Currently, we can only handle object types with system caches. */
    cache = property->oid_catcache_id;
    Assert(cache != -1);

    /* Fetch tuple from syscache and extract namespace attribute. */
    tuple = SearchSysCache1(cache, ObjectIdGetDatum(address->objectId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for cache %d oid %u", cache, address->objectId)));
    oid = DatumGetObjectId(SysCacheGetAttr(cache, tuple, property->attnum_namespace, &isnull));
    Assert(!isnull);
    ReleaseSysCache(tuple);

    return oid;
}

/*
 * Find ObjectProperty structure by class_id.
 */
static const ObjectPropertyType* get_object_property_data(Oid class_id)
{
    uint32 index;

    for (index = 0; index < lengthof(ObjectProperty); index++)
        if (ObjectProperty[index].class_oid == class_id)
            return &ObjectProperty[index];

    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized class id: %u", class_id)));
    return NULL; /* not reached */
}
