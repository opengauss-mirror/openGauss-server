/* -------------------------------------------------------------------------
 *
 * aclchk.cpp
 *	  Routines to check access control permissions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/aclchk.cpp
 *
 * NOTES
 *	  See acl.h.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/gs_client_global_keys.h"
#include "catalog/gs_column_keys.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_directory.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/gs_package.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/gs_global_chain.h"
#include "catalog/gs_global_config.h"
#include "catalog/gs_db_privilege.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/proclang.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/directory.h"
#include "executor/node/nodeModifyTable.h"
#include "foreign/foreign.h"
#include "gs_policy/policy_common.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "pgxc/pgxc.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "auditfuncs.h"
#include "datasource/datasource.h"
#include "storage/proc.h"

/*
 * The information about one Grant/Revoke statement, in internal format: object
 * and grantees names have been turned into Oids, the privilege list is an AclMode bitmask.
 * If 'privileges' is ACL_NO_RIGHTS (the 0 value) and and 'ddl_privileges' is ACL_NO_DDL_RIGHTS and
 * all_privs is true, 'privileges' and 'ddl_privileges' will be internally set to the right kind of
 * ACL_ALL_RIGHTS_* and ACL_ALL_DDL_RIGHTS_* respectively, depending on the object type
 * (NB - this will modify the InternalGrant struct!)
 *
 * Note: 'all_privs', 'privileges' and 'ddl_privileges' represent object-level privileges only.
 * There might also be column-level privilege specifications, which are
 * represented in col_privs and col_ddl_privs (this is a list of untransformed AccessPriv nodes).
 * Column privileges are only valid for objtype ACL_OBJECT_RELATION.
 */

/*
 * Internal format used by ALTER DEFAULT PRIVILEGES.
 */
typedef struct {
    Oid roleid; /* owning role */
    Oid nspid;  /* namespace, or InvalidOid if none */
    /* remaining fields are same as in InternalGrant: */
    bool is_grant;
    GrantObjectType objtype;
    bool all_privs;
    AclMode privileges;
    AclMode ddl_privileges;
    List* grantees;
    bool grant_option;
    DropBehavior behavior;
} InternalDefaultACL;

const struct AclObjectType {
    GrantObjectType objtype;
    AclMode all_privileges;
    AclMode all_ddl_privileges;
    const char* errormsg;
} acl_object_type[] = {
    {ACL_OBJECT_RELATION, (ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE), 
        ACL_ALL_DDL_RIGHTS_RELATION, gettext_noop("invalid privilege type %s for relation")},
    {ACL_OBJECT_SEQUENCE, ACL_ALL_RIGHTS_SEQUENCE, ACL_ALL_DDL_RIGHTS_SEQUENCE, 
        gettext_noop("invalid privilege type %s for sequence")},
    {ACL_OBJECT_DATABASE, ACL_ALL_RIGHTS_DATABASE, ACL_ALL_DDL_RIGHTS_DATABASE, 
        gettext_noop("invalid privilege type %s for database")},
    {ACL_OBJECT_DOMAIN, ACL_ALL_RIGHTS_TYPE, ACL_ALL_DDL_RIGHTS_DOMAIN, 
        gettext_noop("invalid privilege type %s for domain")},
    {ACL_OBJECT_FUNCTION, ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_DDL_RIGHTS_FUNCTION, 
        gettext_noop("invalid privilege type %s for function")},
    {ACL_OBJECT_LANGUAGE, ACL_ALL_RIGHTS_LANGUAGE, ACL_ALL_DDL_RIGHTS_LANGUAGE, 
        gettext_noop("invalid privilege type %s for language")},
    {ACL_OBJECT_PACKAGE, ACL_ALL_RIGHTS_PACKAGE, ACL_ALL_DDL_RIGHTS_PACKAGE, 
        gettext_noop("invalid privilege type %s for package")},
    {ACL_OBJECT_LARGEOBJECT, ACL_ALL_RIGHTS_LARGEOBJECT, ACL_ALL_DDL_RIGHTS_LARGEOBJECT, 
        gettext_noop("invalid privilege type %s for large object")},
    {ACL_OBJECT_NAMESPACE, ACL_ALL_RIGHTS_NAMESPACE, ACL_ALL_DDL_RIGHTS_NAMESPACE, 
        gettext_noop("invalid privilege type %s for schema")},
    {ACL_OBJECT_NODEGROUP, ACL_ALL_RIGHTS_NODEGROUP, ACL_ALL_DDL_RIGHTS_NODEGROUP, 
        gettext_noop("invalid privilege type %s for node group")},
    {ACL_OBJECT_TABLESPACE, ACL_ALL_RIGHTS_TABLESPACE, ACL_ALL_DDL_RIGHTS_TABLESPACE, 
        gettext_noop("invalid privilege type %s for tablespace")},
    {ACL_OBJECT_TYPE, ACL_ALL_RIGHTS_TYPE, ACL_ALL_DDL_RIGHTS_TYPE, 
        gettext_noop("invalid privilege type %s for type")},
    {ACL_OBJECT_FDW, ACL_ALL_RIGHTS_FDW, ACL_ALL_DDL_RIGHTS_FDW, 
        gettext_noop("invalid privilege type %s for foreign-data wrapper")},
    {ACL_OBJECT_FOREIGN_SERVER, ACL_ALL_RIGHTS_FOREIGN_SERVER, ACL_ALL_DDL_RIGHTS_FOREIGN_SERVER, 
        gettext_noop("invalid privilege type %s for foreign server")},
    {ACL_OBJECT_DATA_SOURCE, ACL_ALL_RIGHTS_DATA_SOURCE, ACL_ALL_DDL_RIGHTS_DATA_SOURCE, 
        gettext_noop("invalid privilege type %s for data source")},
    {ACL_OBJECT_GLOBAL_SETTING, ACL_ALL_RIGHTS_KEY, ACL_ALL_DDL_RIGHTS_KEY, 
        gettext_noop("invalid privilege type %s for client master key")},
    {ACL_OBJECT_COLUMN_SETTING, ACL_ALL_RIGHTS_KEY, ACL_ALL_DDL_RIGHTS_KEY, 
        gettext_noop("invalid privilege type %s for column encryption key")},
    {ACL_OBJECT_DIRECTORY, ACL_ALL_RIGHTS_DIRECTORY, ACL_ALL_DDL_RIGHTS_DIRECTORY, 
        gettext_noop("invalid privilege type %s for directory")},
};

const struct AclObjKind {
    AclObjectKind objkind;
    AclMode whole_mask;
    AclMode whole_ddl_mask;
} acl_obj_kind[] = {
    {ACL_KIND_COLUMN, ACL_ALL_RIGHTS_COLUMN, ACL_ALL_DDL_RIGHTS_COLUMN},
    {ACL_KIND_CLASS, ACL_ALL_RIGHTS_RELATION, ACL_ALL_DDL_RIGHTS_RELATION},
    {ACL_KIND_SEQUENCE, ACL_ALL_RIGHTS_SEQUENCE, ACL_ALL_DDL_RIGHTS_SEQUENCE},
    {ACL_KIND_DATABASE, ACL_ALL_RIGHTS_DATABASE, ACL_ALL_DDL_RIGHTS_DATABASE},
    {ACL_KIND_PROC, ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_DDL_RIGHTS_FUNCTION},
    {ACL_KIND_PACKAGE, ACL_ALL_RIGHTS_PACKAGE, ACL_ALL_DDL_RIGHTS_PACKAGE},
    {ACL_KIND_LANGUAGE, ACL_ALL_RIGHTS_LANGUAGE, ACL_ALL_DDL_RIGHTS_LANGUAGE},
    {ACL_KIND_LARGEOBJECT, ACL_ALL_RIGHTS_LARGEOBJECT, ACL_ALL_DDL_RIGHTS_LARGEOBJECT},
    {ACL_KIND_NAMESPACE, ACL_ALL_RIGHTS_NAMESPACE, ACL_ALL_DDL_RIGHTS_NAMESPACE},
    {ACL_KIND_NODEGROUP, ACL_ALL_RIGHTS_NODEGROUP, ACL_ALL_DDL_RIGHTS_NODEGROUP},
    {ACL_KIND_TABLESPACE, ACL_ALL_RIGHTS_TABLESPACE, ACL_ALL_DDL_RIGHTS_TABLESPACE},
    {ACL_KIND_FDW, ACL_ALL_RIGHTS_FDW, ACL_ALL_DDL_RIGHTS_FDW},
    {ACL_KIND_FOREIGN_SERVER, ACL_ALL_RIGHTS_FOREIGN_SERVER, ACL_ALL_DDL_RIGHTS_FOREIGN_SERVER},
    {ACL_KIND_TYPE, ACL_ALL_RIGHTS_TYPE, ACL_ALL_DDL_RIGHTS_TYPE},
    {ACL_KIND_DATA_SOURCE, ACL_ALL_RIGHTS_DATA_SOURCE, ACL_ALL_DDL_RIGHTS_DATA_SOURCE},
    {ACL_KIND_DIRECTORY, ACL_ALL_RIGHTS_DIRECTORY, ACL_ALL_DDL_RIGHTS_DIRECTORY},
    {ACL_KIND_COLUMN_SETTING, ACL_ALL_RIGHTS_KEY, ACL_ALL_DDL_RIGHTS_KEY},
    {ACL_KIND_GLOBAL_SETTING, ACL_ALL_RIGHTS_KEY, ACL_ALL_DDL_RIGHTS_KEY},
    {ACL_KIND_EVENT_TRIGGER, ACL_NO_RIGHTS, ACL_NO_DDL_RIGHTS},   
};

const struct AclClassId {
    Oid classid;
    GrantObjectType objtype;
} acl_class_id[] = {
    {RelationRelationId, ACL_OBJECT_RELATION},
    {DatabaseRelationId, ACL_OBJECT_DATABASE},
    {TypeRelationId, ACL_OBJECT_TYPE},
    {ProcedureRelationId, ACL_OBJECT_FUNCTION},
    {PackageRelationId, ACL_OBJECT_PACKAGE},
    {LanguageRelationId, ACL_OBJECT_LANGUAGE},
    {LargeObjectRelationId, ACL_OBJECT_LARGEOBJECT},
    {NamespaceRelationId, ACL_OBJECT_NAMESPACE},
    {TableSpaceRelationId, ACL_OBJECT_TABLESPACE},
    {ForeignServerRelationId, ACL_OBJECT_FOREIGN_SERVER},
    {ForeignDataWrapperRelationId, ACL_OBJECT_FDW},
    {PgDirectoryRelationId, ACL_OBJECT_DIRECTORY},
    {PgxcGroupRelationId, ACL_OBJECT_NODEGROUP},
    {DataSourceRelationId, ACL_OBJECT_DATA_SOURCE},
    {ClientLogicGlobalSettingsId, ACL_OBJECT_GLOBAL_SETTING},
    {ClientLogicColumnSettingsId, ACL_OBJECT_COLUMN_SETTING},
};

static void ExecGrantStmt_oids(InternalGrant* istmt);
void ExecGrant_Relation(InternalGrant* grantStmt);
static void ExecGrant_Database(InternalGrant* grantStmt);
static void ExecGrant_Fdw(InternalGrant* grantStmt);
static void ExecGrant_ForeignServer(InternalGrant* grantStmt);
static void ExecGrant_Function(InternalGrant* grantStmt);
static void ExecGrant_Package(InternalGrant* istmt);
static void ExecGrant_Language(InternalGrant* grantStmt);
static void ExecGrant_Largeobject(InternalGrant* grantStmt);
static void ExecGrant_Namespace(InternalGrant* grantStmt);
static void ExecGrant_NodeGroup(InternalGrant* grantStmt);
static void ExecGrant_Tablespace(InternalGrant* grantStmt);
static void ExecGrant_Type(InternalGrant* grantStmt);
static void ExecGrant_DataSource(InternalGrant* istmt);
static void ExecGrant_Directory(InternalGrant* istmt);
static void ExecGrant_Cek(InternalGrant *grantStmt);
static void ExecGrant_Cmk(InternalGrant *istmt);

static void SetDefaultACLsInSchemas(InternalDefaultACL* iacls, List* nspnames);
static void SetDefaultACL(InternalDefaultACL* iacls);

static List *objectNamesToOids(GrantObjectType objtype, List *objnames);
static List* objectsInSchemaToOids(GrantObjectType objtype, List* nspnames);
static List* getRelationsInNamespace(Oid namespaceId, char relkind);
static void expand_col_privileges(
    List* colnames, Oid table_oid, AclMode this_privileges, AclMode* col_privileges, int num_col_privileges);
static void expand_all_col_privileges(
    Oid table_oid, Form_pg_class classForm, AclMode this_privileges, AclMode* col_privileges, int num_col_privileges, bool relhasbucket, bool relhasuids);
static AclMode string_to_privilege(const char* privname);
static const char* privilege_to_string(AclMode privilege);
static void restrict_and_check_grant(AclMode* this_privileges, bool is_grant,
    AclMode avail_goptions, AclMode avail_ddl_goptions, bool all_privs, AclMode privileges, AclMode ddl_privileges,
    Oid objectId, Oid grantorId, AclObjectKind objkind, const char* objname, AttrNumber att_number,
    const char* colname);
static AclMode pg_aclmask(
    AclObjectKind objkind, Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how);

extern void check_acl(const Acl* acl);
static void ExecGrantRelationPrivilegesCheck(Form_pg_class tuple, AclMode* privileges, AclMode* ddlprivileges);

#ifdef ACLDEBUG
static void dumpacl(Acl* acl)
{
    int i;
    AclItem* aip = NULL;

    elog(DEBUG2, "acl size = %d, # acls = %d", ACL_SIZE(acl), ACL_NUM(acl));
    aip = ACL_DAT(acl);
    for (i = 0; i < ACL_NUM(acl); ++i)
        elog(DEBUG2, "	acl[%d]: %s", i, DatumGetCString(DirectFunctionCall1(aclitemout, PointerGetDatum(aip + i))));
}
#endif /* ACLDEBUG */

/* if we have a detoasted copy of acl, free it */
#define FREE_DETOASTED_ACL(acl, aclDatum) do {                          \
    if ((acl) && ((Pointer)(acl) != DatumGetPointer((aclDatum)))) {     \
        pfree_ext((acl));                                               \
    }                                                                   \
} while (0)

/*
 * If is_grant is true, adds the given privileges for the list of
 * grantees to the existing old_acl.  If is_grant is false, the
 * privileges for the given grantees are removed from old_acl.
 *
 * NB: the original old_acl is pfree'd.
 */
static Acl* merge_acl_with_grant(Acl* old_acl, bool is_grant, bool grant_option, DropBehavior behavior, List* grantees,
    const AclMode* privileges, Oid grantorId, Oid ownerId)
{
    unsigned modechg;
    ListCell* j = NULL;
    Acl* new_acl = NULL;

    modechg = is_grant ? ACL_MODECHG_ADD : ACL_MODECHG_DEL;

#ifdef ACLDEBUG
    dumpacl(old_acl);
#endif
    new_acl = old_acl;

    foreach (j, grantees) {
        AclItem aclitem;
        AclItem ddl_aclitem;
        Acl* newer_acl = NULL;

        aclitem.ai_grantee = lfirst_oid(j);
        ddl_aclitem.ai_grantee = lfirst_oid(j);

        /*
         * Grant options can only be granted to individual roles, not PUBLIC.
         * The reason is that if a user would re-grant a privilege that he
         * held through PUBLIC, and later the user is removed, the situation
         * is impossible to clean up.
         */
        if (is_grant && grant_option && aclitem.ai_grantee == ACL_ID_PUBLIC)
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("invalid grant operation"), errdetail("Grant options can only be granted to roles."),
                    errcause("Grant options cannnot be granted to public."),
                        erraction("Grant grant options to roles.")));

        aclitem.ai_grantor = grantorId;
        ddl_aclitem.ai_grantor = grantorId;

        /*
         * The asymmetry in the conditions here comes from the spec.  In
         * GRANT, the grant_option flag signals WITH GRANT OPTION, which means
         * to grant both the basic privilege and its grant option. But in
         * REVOKE, plain revoke revokes both the basic privilege and its grant
         * option, while REVOKE GRANT OPTION revokes only the option.
         */
        ACLITEM_SET_PRIVS_GOPTIONS(aclitem,
            (is_grant || !grant_option) ? privileges[DML_PRIVS_INDEX] : ACL_NO_RIGHTS,
            (!is_grant || grant_option) ? privileges[DML_PRIVS_INDEX] : ACL_NO_RIGHTS);

        ACLITEM_SET_PRIVS_GOPTIONS(ddl_aclitem,
            (is_grant || !grant_option) ? REMOVE_DDL_FLAG(privileges[DDL_PRIVS_INDEX]) : ACL_NO_RIGHTS,
            (!is_grant || grant_option) ? REMOVE_DDL_FLAG(privileges[DDL_PRIVS_INDEX]) : ACL_NO_RIGHTS);
        ddl_aclitem.ai_privs = ADD_DDL_FLAG(ddl_aclitem.ai_privs);

        newer_acl = aclupdate(new_acl, &aclitem, modechg, ownerId, behavior);
        newer_acl = aclupdate(newer_acl, &ddl_aclitem, modechg, ownerId, behavior);

        /* avoid memory leak when there are many grantees */
        pfree_ext(new_acl);
        new_acl = newer_acl;

#ifdef ACLDEBUG
        dumpacl(new_acl);
#endif
    }

    return new_acl;
}
/*
 * Restrict the privileges to what we can actually grant, and emit
 * the standards-mandated warning and error messages.
 */
static void restrict_and_check_grant(AclMode* this_privileges, bool is_grant,
    AclMode avail_goptions, AclMode avail_ddl_goptions, bool all_privs, AclMode privileges, AclMode ddl_privileges,
    Oid objectId, Oid grantorId, AclObjectKind objkind, const char* objname, AttrNumber att_number, const char* colname)
{
    AclMode whole_mask;
    AclMode whole_ddl_mask;
    bool kind_flag = false;

    for (size_t idx = 0; idx < sizeof(acl_obj_kind) / sizeof(acl_obj_kind[0]); idx++) {
        if (acl_obj_kind[idx].objkind == objkind) {
            whole_mask = acl_obj_kind[idx].whole_mask;
            whole_ddl_mask = acl_obj_kind[idx].whole_ddl_mask;
            kind_flag = true;
            break;
        }
    }
    
    if (!kind_flag) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unrecognized object kind: %d", objkind), errdetail("N/A"),
                errcause("The object type is not supported for GRANT/REVOKE."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
        /* not reached, but keep compiler quiet */
        whole_mask = ACL_NO_RIGHTS;
        whole_ddl_mask = ACL_NO_DDL_RIGHTS;
    }

    if ((whole_mask == ACL_NO_RIGHTS) && (objkind == ACL_KIND_EVENT_TRIGGER)) {
        elog(ERROR, "grantable rights not supported for event triggers");
        /* not reached, but keep compiler quiet */
        return ;
    }
    /*
     * If we found no grant options, consider whether to issue a hard error.
     * Per spec, having any privilege at all on the object will get you by
     * here.
     */
    if (avail_goptions == ACL_NO_RIGHTS && REMOVE_DDL_FLAG(avail_ddl_goptions) == ACL_NO_RIGHTS) {
        if (pg_aclmask(objkind, objectId, att_number, grantorId,
            whole_mask | ACL_GRANT_OPTION_FOR(whole_mask), ACLMASK_ANY) == ACL_NO_RIGHTS &&
            pg_aclmask(objkind, objectId, att_number, grantorId,
            whole_ddl_mask | ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(whole_ddl_mask)), ACLMASK_ANY) == ACL_NO_RIGHTS) {
            if (objkind == ACL_KIND_COLUMN && colname != NULL)
                aclcheck_error_col(ACLCHECK_NO_PRIV, objkind, objname, colname);
            else
                aclcheck_error(ACLCHECK_NO_PRIV, objkind, objname);
        }
    }

    /*
     * Restrict the operation to what we can actually grant or revoke, and
     * issue a warning if appropriate.	(For REVOKE this isn't quite what the
     * spec says to do: the spec seems to want a warning only if no privilege
     * bits actually change in the ACL. In practice that behavior seems much
     * too noisy, as well as inconsistent with the GRANT case.)
     */
    this_privileges[DML_PRIVS_INDEX] = privileges & ACL_OPTION_TO_PRIVS(avail_goptions);
    /* remove ddl privileges flag from Aclitem */
    ddl_privileges = REMOVE_DDL_FLAG(ddl_privileges);
    this_privileges[DDL_PRIVS_INDEX] = ddl_privileges & ACL_OPTION_TO_PRIVS(avail_ddl_goptions);

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    int level = WARNING;
#else
    int level = ERROR;
#endif

    if (is_grant) {
        if (this_privileges[DML_PRIVS_INDEX] == 0 && this_privileges[DDL_PRIVS_INDEX] == 0) {
            if (objkind == ACL_KIND_COLUMN && colname != NULL)
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                        errmsg("no privileges were granted for column \"%s\" of relation \"%s\"", colname, objname)));
            else
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                        errmsg("no privileges were granted for \"%s\"", objname)));
        } else if (!all_privs && ((this_privileges[DML_PRIVS_INDEX] != privileges) ||
            (this_privileges[DDL_PRIVS_INDEX] != ddl_privileges))) {
            if (objkind == ACL_KIND_COLUMN && colname != NULL)
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                        errmsg(
                            "not all privileges were granted for column \"%s\" of relation \"%s\"", colname, objname)));
            else
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                        errmsg("not all privileges were granted for \"%s\"", objname)));
        }
    } else {
        if (this_privileges[DML_PRIVS_INDEX] == 0 && this_privileges[DDL_PRIVS_INDEX] == 0) {
            if (objkind == ACL_KIND_COLUMN && colname != NULL)
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
                        errmsg(
                            "no privileges could be revoked for column \"%s\" of relation \"%s\"", colname, objname)));
            else
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
                        errmsg("no privileges could be revoked for \"%s\"", objname)));
        } else if (!all_privs && ((this_privileges[DML_PRIVS_INDEX] != privileges) ||
            (this_privileges[DDL_PRIVS_INDEX] != ddl_privileges))) {
            if (objkind == ACL_KIND_COLUMN && colname != NULL)
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
                        errmsg("not all privileges could be revoked for column \"%s\" of relation \"%s\"",
                            colname,
                            objname)));
            else
                ereport(level,
                    (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
                        errmsg("not all privileges could be revoked for \"%s\"", objname)));
        }
    }

    /* add ddl privileges flag to Aclitem */
    this_privileges[DDL_PRIVS_INDEX] = ADD_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]);
}

void check_nodegroup_privilege(Oid roleid, Oid ownerId, AclMode mode)
{
    if (!in_logic_cluster()) {
        return;
    }

    if (roleid == ownerId && mode == ACL_USAGE) {
        return;
    }

    AclResult aclresult;
    Oid group_oid = get_pgxc_logic_groupoid(ownerId);
    if (!OidIsValid(group_oid)) {
        // owner don't belong to any virtual cluster, so ignore!
        return;
    }

    aclresult = pg_nodegroup_aclcheck(group_oid, roleid, mode);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, ACL_KIND_NODEGROUP, get_pgxc_groupname(group_oid));
    }
}

/*
 * Get all object oids from GrantStmt
 */
List* GrantStmtGetObjectOids(GrantStmt* stmt)
{
    List* objects = NIL;
    switch (stmt->targtype) {
        case ACL_TARGET_OBJECT:
            objects = objectNamesToOids(stmt->objtype, stmt->objects);
            break;
        case ACL_TARGET_ALL_IN_SCHEMA:
            objects = objectsInSchemaToOids(stmt->objtype, stmt->objects);
            break;
            /* ACL_TARGET_DEFAULTS should not be seen here */
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized GrantStmt.targtype: %d", (int)stmt->targtype), errdetail("N/A"),
                    errcause("The target type is not supported for GRANT/REVOKE."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported target types.")));
    }
    return objects;
}

/*
 * Called to execute the utility commands GRANT and REVOKE
 */
void ExecuteGrantStmt(GrantStmt* stmt)
{
    InternalGrant istmt;
    ListCell* cell = NULL;
    const char* errormsg = NULL;
    AclMode all_privileges;
    AclMode all_ddl_privileges;
    bool type_flag = false;

    /*
     * Turn the regular GrantStmt into the InternalGrant form.
     */
    istmt.is_grant = stmt->is_grant;
    istmt.objtype = stmt->objtype;

    /* Collect the OIDs of the target objects */
    istmt.objects = GrantStmtGetObjectOids(stmt);

    /* all_privs to be filled below */
    /* privileges and ddl_privileges to be filled below */
    istmt.col_privs = NIL; /* may get filled below */
    istmt.col_ddl_privs = NIL; /* may get filled below */
    istmt.grantees = NIL;  /* filled below */
    istmt.grant_option = stmt->grant_option;
    istmt.behavior = stmt->behavior;

    /*
     * Convert the PrivGrantee list into an Oid list.  Note that at this point
     * we insert an ACL_ID_PUBLIC into the list if an empty role name is
     * detected (which is what the grammar uses if PUBLIC is found), so
     * downstream there shouldn't be any additional work needed to support
     * this case.
     */
    foreach (cell, stmt->grantees) {
        PrivGrantee* grantee = (PrivGrantee*)lfirst(cell);

        if (grantee->rolname == NULL) {
            /* In securitymode, grant to public operation is forbidden */
            if (stmt->is_grant && isSecurityMode && !IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade &&
                !u_sess->exec_cxt.extension_is_valid) {
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid grant operation"),
                        errdetail("Grant to public operation is forbidden in security mode."),
                            errcause("Grant to public operation is forbidden in security mode."),
                                erraction("Don't grant to public in security mode.")));
            }
            istmt.grantees = lappend_oid(istmt.grantees, ACL_ID_PUBLIC);
        } else
            istmt.grantees = lappend_oid(istmt.grantees, get_role_oid(grantee->rolname, false));
    }

    /*
     * Convert stmt->privileges, a list of AccessPriv nodes, into an AclMode
     * bitmask.  Note: objtype can't be ACL_OBJECT_COLUMN.
     */
    for (size_t idx = 0; idx < sizeof(acl_object_type) / sizeof(acl_object_type[0]); idx++) {
        if (acl_object_type[idx].objtype == stmt->objtype) {
            all_privileges = acl_object_type[idx].all_privileges;
            all_ddl_privileges = acl_object_type[idx].all_ddl_privileges;
            errormsg = acl_object_type[idx].errormsg;
            type_flag = true;
            break;
        }
    }
    if (!type_flag) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unrecognized object type"), errdetail("unrecognized GrantStmt.objtype: %d", (int)stmt->objtype),
                errcause("The object type is not supported for GRANT/REVOKE."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
        /* keep compiler quiet */
        all_privileges = ACL_NO_RIGHTS;
        all_ddl_privileges = ACL_NO_DDL_RIGHTS;
        errormsg = NULL;
    }

    if (stmt->privileges == NIL) {
        istmt.all_privs = true;

        /*
         * will be turned into ACL_ALL_RIGHTS_* by the internal routines
         * depending on the object type
         */
        istmt.privileges = ACL_NO_RIGHTS;
        istmt.ddl_privileges = ACL_NO_DDL_RIGHTS;
    } else {
        istmt.all_privs = false;
        istmt.privileges = ACL_NO_RIGHTS;
        istmt.ddl_privileges = ACL_NO_DDL_RIGHTS;

        foreach (cell, stmt->privileges) {
            AccessPriv* privnode = (AccessPriv*)lfirst(cell);
            AclMode priv;

            /*
             * If it's a column-level specification, we just set it aside in
             * col_privs for the moment; but insist it's for a relation.
             */
            if (privnode->cols) {
                if (stmt->objtype != ACL_OBJECT_RELATION)
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg("invalid grant/revoke operation"),
                            errdetail("Column privileges are only valid for relations."),
                                errcause("Column privileges are only valid for relations in GRANT/REVOKE."),
                                    erraction("Use the column privileges only for relations.")));
                
                if (privnode->priv_name == NULL) {
                    istmt.col_ddl_privs = lappend(istmt.col_ddl_privs, privnode);
                    istmt.col_privs = lappend(istmt.col_privs, privnode);
                } else {
                    priv = string_to_privilege(privnode->priv_name);
                    if (ACLMODE_FOR_DDL(priv)) {
                        istmt.col_ddl_privs = lappend(istmt.col_ddl_privs, privnode);
                    } else {
                        istmt.col_privs = lappend(istmt.col_privs, privnode);
                    }
                }
                
                continue;
            }

            if (privnode->priv_name == NULL) { /* parser mistake? */
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid AccessPriv node"), errdetail("AccessPriv node must specify privilege or columns"),
                        errcause("System error."), erraction("Contact engineer to support.")));
            }

            priv = string_to_privilege(privnode->priv_name);
            if (stmt->objtype == ACL_OBJECT_NODEGROUP && strcmp(privnode->priv_name, "create") == 0) {
                priv = ACL_ALL_RIGHTS_NODEGROUP;
            }

            if (ACLMODE_FOR_DDL(priv)) {
                if (priv & ~((AclMode)all_ddl_privileges)) {
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg(errormsg, privilege_to_string(priv)), errdetail("N/A"),
                            errcause("The privilege type is not supported for the object."),
                                erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types "
                                    "for the object type.")));
                }

                istmt.ddl_privileges |= priv;
            } else {
                if (priv & ~((AclMode)all_privileges)) {
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg(errormsg, privilege_to_string(priv)), errdetail("N/A"),
                            errcause("The privilege type is not supported for the object."),
                                erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types "
                                    "for the object type.")));
                }

                istmt.privileges |= priv;
            }
        }
    }

    ExecGrantStmt_oids(&istmt);
}

/*
 * ExecGrantStmt_oids
 *
 * Internal entry point for granting and revoking privileges.
 */
static void ExecGrantStmt_oids(InternalGrant* istmt)
{
    switch (istmt->objtype) {
        case ACL_OBJECT_RELATION:
        case ACL_OBJECT_SEQUENCE:
            ExecGrant_Relation(istmt);
            break;
        case ACL_OBJECT_DATABASE:
            ExecGrant_Database(istmt);
            break;
        case ACL_OBJECT_DOMAIN:
        case ACL_OBJECT_TYPE:
            ExecGrant_Type(istmt);
            break;
        case ACL_OBJECT_FDW:
            ExecGrant_Fdw(istmt);
            break;
        case ACL_OBJECT_FOREIGN_SERVER:
            ExecGrant_ForeignServer(istmt);
            break;
        case ACL_OBJECT_FUNCTION:
            ExecGrant_Function(istmt);
            break;
        case ACL_OBJECT_PACKAGE:
            ExecGrant_Package(istmt);
            break;
        case ACL_OBJECT_LANGUAGE:
            ExecGrant_Language(istmt);
            break;
        case ACL_OBJECT_LARGEOBJECT:
            ExecGrant_Largeobject(istmt);
            break;
        case ACL_OBJECT_NAMESPACE:
            ExecGrant_Namespace(istmt);
            break;
        case ACL_OBJECT_NODEGROUP:
            ExecGrant_NodeGroup(istmt);
            break;
        case ACL_OBJECT_TABLESPACE:
            ExecGrant_Tablespace(istmt);
            break;
        case ACL_OBJECT_DATA_SOURCE:
            ExecGrant_DataSource(istmt);
            break;
        case ACL_OBJECT_GLOBAL_SETTING:
            ExecGrant_Cmk(istmt);
            break;
        case ACL_OBJECT_COLUMN_SETTING:
            ExecGrant_Cek(istmt);
            break;
        case ACL_OBJECT_DIRECTORY:
            ExecGrant_Directory(istmt);
            break;
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized GrantStmt.objtype: %d", (int)istmt->objtype), errdetail("N/A"),
                    errcause("The object type is not supported for GRANT/REVOKE."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
    }

    /*
     * Pass the info to event triggers about the just-executed GRANT.  Note
     * that we prefer to do it after actually executing it, because that gives
     * the functions a chance to adjust the istmt with privileges actually
     * granted.
     */
    if (EventTriggerSupportsGrantObjectType(istmt->objtype))
        EventTriggerCollectGrant(istmt);
}

static void get_global_objects(const List *objnames, List **objects)
{
    KeyCandidateList clist = GlobalSettingGetCandidates(objnames, false);
    if (!clist) {
        ereport(ERROR, (errmodule(MOD_SEC_FE), errcode(ERRCODE_UNDEFINED_KEY),
            errmsg("undefined client master key"), errdetail("client master key \"%s\" does not exist",
                NameListToString(objnames)),
                errcause("The client master key does not exist."),
                    erraction("Check whether the client master key exists.")));
    }
    for (; clist; clist = clist->next) {
        *objects = lappend_oid(*objects, clist->oid);
    }
}

static void get_column_objects(const List *objnames, List **objects)
{
    KeyCandidateList clist = CeknameGetCandidates(objnames, false);
    if (!clist) {
        ereport(ERROR, (errmodule(MOD_SEC_FE), errcode(ERRCODE_UNDEFINED_KEY),
            errmsg("undefined column encryption key"),
                errdetail("column encryption key \"%s\" does not exist", NameListToString(objnames)),
                    errcause("The column encryption key does not exist."),
                        erraction("Check whether the column encryption key exists.")));
    }
    for (; clist; clist = clist->next) {
        *objects = lappend_oid(*objects, clist->oid);
    }
}

/*
 * objectNamesToOids
 *
 * Turn a list of object names of a given type into an Oid list.
 *
 * XXX: This function doesn't take any sort of locks on the objects whose
 * names it looks up.  In the face of concurrent DDL, we might easily latch
 * onto an old version of an object, causing the GRANT or REVOKE statement
 * to fail.
 */
static List *objectNamesToOids(GrantObjectType objtype, List *objnames)
{
    List* objects = NIL;
    ListCell* cell = NULL;

    Assert(objnames != NIL);

    switch (objtype) {
        case ACL_OBJECT_RELATION:
        case ACL_OBJECT_SEQUENCE:
            foreach (cell, objnames) {
                RangeVar* relvar = (RangeVar*)lfirst(cell);
                Oid relOid;

                relOid = RangeVarGetRelid(relvar, NoLock, false);
                objects = lappend_oid(objects, relOid);
            }
            break;
        case ACL_OBJECT_DATABASE:
            foreach (cell, objnames) {
                char* dbname = strVal(lfirst(cell));
                Oid dbid;

                dbid = get_database_oid(dbname, false);
                objects = lappend_oid(objects, dbid);
            }
            break;
        case ACL_OBJECT_DOMAIN:
        case ACL_OBJECT_TYPE:
            foreach (cell, objnames) {
                List* typname = (List*)lfirst(cell);
                Oid oid;

                oid = typenameTypeId(NULL, makeTypeNameFromNameList(typname));
                objects = lappend_oid(objects, oid);
#ifndef ENABLE_MULTIPLE_NODES
                /* don't allow to alter package or procedure type */
                if (IsPackageDependType(oid, InvalidOid)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmodule(MOD_PLSQL),
                            errmsg("Not allowed to GRANT/REVOKE type \"%s\"", NameListToString(typname)),
                            errdetail("\"%s\" is a package or procedure type", NameListToString(typname)),
                            errcause("feature not supported"),
                            erraction("check type name")));
                }
#endif
            }
            break;
        case ACL_OBJECT_FUNCTION:
            foreach (cell, objnames) {
                FuncWithArgs* func = (FuncWithArgs*)lfirst(cell);
                Oid funcid;

                funcid = LookupFuncNameTypeNames(func->funcname, func->funcargs, false);
                objects = lappend_oid(objects, funcid);
            }
            break;
        case ACL_OBJECT_PACKAGE:
            foreach (cell, objnames) {
                Oid pkgid;
                List* pkgname = (List*)lfirst(cell);
                pkgid = PackageNameListGetOid(pkgname,false);
                objects = lappend_oid(objects, pkgid);
            }
            break;
        case ACL_OBJECT_LANGUAGE:
            foreach (cell, objnames) {
                char* langname = strVal(lfirst(cell));
                Oid oid;

                oid = get_language_oid(langname, false);
                objects = lappend_oid(objects, oid);
            }
            break;
        case ACL_OBJECT_LARGEOBJECT:
            foreach (cell, objnames) {
                Oid lobjOid = oidparse((Node*)lfirst(cell));

                if (!LargeObjectExists(lobjOid))
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("large object %u does not exist", lobjOid), errdetail("N/A"),
                            errcause("The large object does not exist."),
                                erraction("Check whether the large object exists.")));

                objects = lappend_oid(objects, lobjOid);
            }
            break;
        case ACL_OBJECT_NAMESPACE:
            foreach (cell, objnames) {
                char* nspname = strVal(lfirst(cell));
                Oid oid;

                oid = get_namespace_oid(nspname, false);
                objects = lappend_oid(objects, oid);
            }
            break;
        case ACL_OBJECT_NODEGROUP:
            foreach (cell, objnames) {
                const char* group_name = strVal(lfirst(cell));
                Oid group_oid = get_pgxc_groupoid(group_name, false);
                objects = lappend_oid(objects, group_oid);

                if (!in_logic_cluster()) {
                    continue;
                }

                Oid dest_group(0);
                char in_redis = get_pgxc_group_redistributionstatus(group_oid);
                bool redis_valid_oid = (in_redis == PGXC_REDISTRIBUTION_SRC_GROUP) && 
                                        (OidIsValid(dest_group = PgxcGroupGetRedistDestGroupOid()));
                if (redis_valid_oid) {
                    objects = lappend_oid(objects, dest_group);
                }
            }
            break;
        case ACL_OBJECT_TABLESPACE:
            foreach (cell, objnames) {
                char* spcname = strVal(lfirst(cell));
                Oid spcoid;

                spcoid = get_tablespace_oid(spcname, false);
                objects = lappend_oid(objects, spcoid);
            }
            break;
        case ACL_OBJECT_FDW:
            foreach (cell, objnames) {
                char* fdwname = strVal(lfirst(cell));
                Oid fdwid = get_foreign_data_wrapper_oid(fdwname, false);

                objects = lappend_oid(objects, fdwid);
            }
            break;
        case ACL_OBJECT_FOREIGN_SERVER:
            foreach (cell, objnames) {
                char* srvname = strVal(lfirst(cell));
                Oid srvid = get_foreign_server_oid(srvname, false);

                objects = lappend_oid(objects, srvid);
            }
            break;
        case ACL_OBJECT_DATA_SOURCE:
            foreach (cell, objnames) {
                const char* srcname = strVal(lfirst(cell));
                Oid srcid = get_data_source_oid(srcname, false);

                objects = lappend_oid(objects, srcid);
            }
            break;
        case ACL_OBJECT_DIRECTORY:
            foreach (cell, objnames) {
                char* dirname = strVal(lfirst(cell));
                Oid diroid;

                diroid = get_directory_oid(dirname, false);
                objects = lappend_oid(objects, diroid);
            }
            break;
        case ACL_OBJECT_GLOBAL_SETTING:
            get_global_objects(objnames, &objects);
            break;
        case ACL_OBJECT_COLUMN_SETTING:
            get_column_objects(objnames, &objects);
            break;

        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized GrantStmt.objtype: %d", (int)objtype), errdetail("N/A"),
                    errcause("The object type is not supported for GRANT/REVOKE."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
    }

    return objects;
}

/*
 * objectsInSchemaToOids
 *
 * Find all objects of a given type in specified schemas, and make a list
 * of their Oids.  We check USAGE privilege on the schemas, but there is
 * no privilege checking on the individual objects here.
 */
static List* objectsInSchemaToOids(GrantObjectType objtype, List* nspnames)
{
    List* objects = NIL;
    ListCell* cell = NULL;

    foreach (cell, nspnames) {
        char* nspname = strVal(lfirst(cell));
        Oid namespaceId;
        List* objs = NIL;

        namespaceId = LookupExplicitNamespace(nspname);

        switch (objtype) {
            case ACL_OBJECT_RELATION:
                /* Process regular tables, views and foreign tables */
                objs = getRelationsInNamespace(namespaceId, RELKIND_RELATION);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_VIEW);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_CONTQUERY);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_MATVIEW);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_FOREIGN_TABLE);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_STREAM);
                objects = list_concat(objects, objs);
                break;
            case ACL_OBJECT_SEQUENCE:
                objs = getRelationsInNamespace(namespaceId, RELKIND_SEQUENCE);
                objects = list_concat(objects, objs);
                objs = getRelationsInNamespace(namespaceId, RELKIND_LARGE_SEQUENCE);
                objects = list_concat(objects, objs);
                break;
            case ACL_OBJECT_FUNCTION: {
                ScanKeyData key[1];
                Relation rel = NULL;
                TableScanDesc scan = NULL;
                HeapTuple tuple = NULL;

                ScanKeyInit(
                    &key[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));

                rel = heap_open(ProcedureRelationId, AccessShareLock);
                scan = heap_beginscan(rel, SnapshotNow, 1, key);

                while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                    objects = lappend_oid(objects, HeapTupleGetOid(tuple));
                }

                heap_endscan(scan);
                heap_close(rel, AccessShareLock);
            }
                break;
            case ACL_OBJECT_PACKAGE: {
                ScanKeyData key[1];
                Relation rel = NULL;
                TableScanDesc scan = NULL;
                HeapTuple tuple = NULL;
                ScanKeyInit(
                    &key[0], Anum_gs_package_pkgnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
                rel = heap_open(PackageRelationId, AccessShareLock);
                scan = heap_beginscan(rel, SnapshotNow, 1, key);
                while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                    objects = lappend_oid(objects, HeapTupleGetOid(tuple));
                }
                heap_endscan(scan);
                heap_close(rel, AccessShareLock);
            }
            break;
            case ACL_OBJECT_GLOBAL_SETTING: {
                ScanKeyData key[1];
                Relation rel;
                TableScanDesc scan;
                HeapTuple tuple;

                ScanKeyInit(&key[0], Anum_gs_client_global_keys_key_namespace, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(namespaceId));

                rel = heap_open(ClientLogicGlobalSettingsId, AccessShareLock);
                scan = heap_beginscan(rel, SnapshotNow, 1, key);

                while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                    objects = lappend_oid(objects, HeapTupleGetOid(tuple));
                }

                heap_endscan(scan);
                heap_close(rel, AccessShareLock);
            }
                break;
            case ACL_OBJECT_COLUMN_SETTING: {
                ScanKeyData key[1];
                Relation rel;
                TableScanDesc scan;
                HeapTuple tuple;

                ScanKeyInit(&key[0], Anum_gs_column_keys_key_namespace, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(namespaceId));

                rel = heap_open(ClientLogicColumnSettingsId, AccessShareLock);
                scan = heap_beginscan(rel, SnapshotNow, 1, key);

                while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                    objects = lappend_oid(objects, HeapTupleGetOid(tuple));
                }

                heap_endscan(scan);
                heap_close(rel, AccessShareLock);
            }
                break;
            default:
                /* should not happen */
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized GrantStmt.objtype: %d", (int)objtype), errdetail("N/A"),
                        errcause("The object type is not supported for GRANT/REVOKE."),
                            erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
        }
    }

    return objects;
}

/*
 * getRelationsInNamespace
 *
 * Return Oid list of relations in given namespace filtered by relation kind
 */
static List* getRelationsInNamespace(Oid namespaceId, char relkind)
{
    List* relations = NIL;
    ScanKeyData key[2];
    Relation rel = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;

    ScanKeyInit(&key[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
    ScanKeyInit(&key[1], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));

    rel = heap_open(RelationRelationId, AccessShareLock);
    scan = heap_beginscan(rel, SnapshotNow, 2, key);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        relations = lappend_oid(relations, HeapTupleGetOid(tuple));
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    return relations;
}

static void DeconstructOptions(const List* options, List** rolenames, List** nspnames, DefElem** drolenames, DefElem** dnspnames)
{
    ListCell* cell = NULL;
    foreach (cell, options) {
        DefElem *defel = (DefElem *)lfirst(cell);

        if (strcmp(defel->defname, "schemas") == 0) {
            if (*dnspnames != NULL) {
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("redundant options"), errdetail("the syntax \"schemas\" is redundant"),
                        errcause("The syntax \"schemas\" is redundant in ALTER DEFAULT PRIVILEGES statement."),
                            erraction("Check ALTER DEFAULT PRIVILEGES syntax.")));
            }
            *dnspnames = defel;
        } else if (strcmp(defel->defname, "roles") == 0) {
            if (*drolenames != NULL) {
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("redundant options"), errdetail("the syntax \"roles\" is redundant"),
                            errcause("The syntax \"roles\" is redundant in ALTER DEFAULT PRIVILEGES statement."),
                                erraction("Check ALTER DEFAULT PRIVILEGES syntax.")));
            }
            *drolenames = defel;
        } else {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("option \"%s\" not recognized", defel->defname), errdetail("N/A"),
                    errcause("The option in ALTER DEFAULT PRIVILEGES statement is not supported."),
                        erraction("Check ALTER DEFAULT PRIVILEGES syntax.")));
        }
    }

    if (*dnspnames != NULL)
        *nspnames = (List *)(*dnspnames)->arg;
    if (*drolenames != NULL)
        *rolenames = (List *)(*drolenames)->arg;
}

/*
 * ALTER DEFAULT PRIVILEGES statement
 */
void ExecAlterDefaultPrivilegesStmt(AlterDefaultPrivilegesStmt* stmt)
{
    GrantStmt* action = stmt->action;
    InternalDefaultACL iacls;
    ListCell* cell = NULL;
    List* rolenames = NIL;
    List* nspnames = NIL;
    DefElem* drolenames = NULL;
    DefElem* dnspnames = NULL;
    AclMode all_privileges;
    AclMode all_ddl_privileges;
    const char* errormsg = NULL;

    /* Deconstruct the "options" part of the statement */
    DeconstructOptions(stmt->options, &rolenames, &nspnames, &drolenames, &dnspnames);

    /* Prepare the InternalDefaultACL representation of the statement */
    /* roleid to be filled below */
    /* nspid to be filled in SetDefaultACLsInSchemas */
    iacls.is_grant = action->is_grant;
    iacls.objtype = action->objtype;
    /* all_privs to be filled below */
    /* privileges to be filled below */
    iacls.grantees = NIL; /* filled below */
    iacls.grant_option = action->grant_option;
    iacls.behavior = action->behavior;

    /*
     * Convert the PrivGrantee list into an Oid list.  Note that at this point
     * we insert an ACL_ID_PUBLIC into the list if an empty role name is
     * detected (which is what the grammar uses if PUBLIC is found), so
     * downstream there shouldn't be any additional work needed to support
     * this case.
     */
    foreach (cell, action->grantees) {
        PrivGrantee* grantee = (PrivGrantee*)lfirst(cell);

        if (grantee->rolname == NULL)
            iacls.grantees = lappend_oid(iacls.grantees, ACL_ID_PUBLIC);
        else
            iacls.grantees = lappend_oid(iacls.grantees, get_role_oid(grantee->rolname, false));
    }

    /*
     * Convert action->privileges, a list of privilege strings, into an
     * AclMode bitmask.
     */
    switch (action->objtype) {
        case ACL_OBJECT_RELATION:
            all_privileges = ACL_ALL_RIGHTS_RELATION;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_RELATION;
            errormsg = gettext_noop("invalid privilege type %s for relation");
            break;
        case ACL_OBJECT_SEQUENCE:
            all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_SEQUENCE;
            errormsg = gettext_noop("invalid privilege type %s for sequence");
            break;
        case ACL_OBJECT_FUNCTION:
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_FUNCTION;
            errormsg = gettext_noop("invalid privilege type %s for function");
            break;
        case ACL_OBJECT_PACKAGE:
            all_privileges = ACL_ALL_RIGHTS_PACKAGE;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_PACKAGE;
            errormsg = gettext_noop("invalid privilege type %s for package");
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("alter default privileges is not support package yet."), errdetail("N/A"),
                    errcause("alter default privileges is not support package yet."),
                        erraction("N/A")));
            break;
        case ACL_OBJECT_TYPE:
            all_privileges = ACL_ALL_RIGHTS_TYPE;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_TYPE;
            errormsg = gettext_noop("invalid privilege type %s for type");
            break;
        case ACL_OBJECT_GLOBAL_SETTING:
            all_privileges = ACL_ALL_RIGHTS_KEY;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_KEY;
            errormsg = gettext_noop("invalid privilege type %s for client master key");
            break;
        case ACL_OBJECT_COLUMN_SETTING:
            all_privileges = ACL_ALL_RIGHTS_KEY;
            all_ddl_privileges = ACL_ALL_DDL_RIGHTS_KEY;
            errormsg = gettext_noop("invalid privilege type %s for column encryption key");
            break;
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized GrantStmt.objtype: %d", (int)action->objtype), errdetail("N/A"),
                    errcause("The object type is not supported for ALTER DEFAULT PRIVILEGES."),
                        erraction("Check ALTER DEFAULT PRIVILEGES syntax to obtain the supported object types.")));
            /* keep compiler quiet */
            all_privileges = ACL_NO_RIGHTS;
            all_ddl_privileges = ACL_NO_DDL_RIGHTS;
            errormsg = NULL;
    }

    if (action->privileges == NIL) {
        iacls.all_privs = true;

        /*
         * will be turned into ACL_ALL_RIGHTS_* by the internal routines
         * depending on the object type
         */
        iacls.privileges = ACL_NO_RIGHTS;
        iacls.ddl_privileges = ACL_NO_DDL_RIGHTS;
    } else {
        iacls.all_privs = false;
        iacls.privileges = ACL_NO_RIGHTS;
        iacls.ddl_privileges = ACL_NO_DDL_RIGHTS;

        foreach (cell, action->privileges) {
            AccessPriv* privnode = (AccessPriv*)lfirst(cell);
            AclMode priv;

            if (privnode->cols != NULL) {
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid alter default privileges operation"),
                        errdetail("Default privileges cannot be set for columns."),
                            errcause("Default privileges cannot be set for columns."),
                                erraction("Check ALTER DEFAULT PRIVILEGES syntax.")));
            }

            if (privnode->priv_name == NULL) { /* parser mistake? */
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid AccessPriv node"), errdetail("AccessPriv node must specify privilege"),
                     errcause("System error."), erraction("Contact engineer to support.")));
            }
            priv = string_to_privilege(privnode->priv_name);

            if (ACLMODE_FOR_DDL(priv)) {
                if (priv & ~((AclMode)all_ddl_privileges)) {
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg(errormsg, privilege_to_string(priv)), errdetail("N/A"),
                            errcause("The privilege type is not supported for the object type."),
                                erraction("Check ALTER DEFAULT PRIVILEGES syntax to obtain the supported "
                                    "privilege types for the object type.")));
                }

                iacls.ddl_privileges |= priv;
            } else {
                if (priv & ~((AclMode)all_privileges)) {
                    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg(errormsg, privilege_to_string(priv)), errdetail("N/A"),
                            errcause("The privilege type is not supported for the object type."),
                                erraction("Check ALTER DEFAULT PRIVILEGES syntax to obtain the supported "
                                    "privilege types for the object type.")));
                }

                iacls.privileges |= priv;
            }
        }
    }

    if (rolenames == NIL) {
        /* Set permissions for myself */
        iacls.roleid = GetUserId();

        SetDefaultACLsInSchemas(&iacls, nspnames);
    } else {
        /* Look up the role OIDs and do permissions checks */
        ListCell* rolecell = NULL;

        foreach (rolecell, rolenames) {
            char* rolename = strVal(lfirst(rolecell));

            iacls.roleid = get_role_oid(rolename, false);

            /*
             * We insist that calling user be a member of each target role. If
             * he has that, he could become that role anyway via SET ROLE, so
             * FOR ROLE is just a syntactic convenience and doesn't give any
             * special privileges.
             */
            check_is_member_of_role(GetUserId(), iacls.roleid);

            SetDefaultACLsInSchemas(&iacls, nspnames);
        }
    }
}

/*
 * Process ALTER DEFAULT PRIVILEGES for a list of target schemas
 *
 * All fields of *iacls except nspid were filled already
 */
static void SetDefaultACLsInSchemas(InternalDefaultACL* iacls, List* nspnames)
{
    if (nspnames == NIL) {
        /* Set database-wide permissions if no schema was specified */
        iacls->nspid = InvalidOid;

        SetDefaultACL(iacls);
    } else {
        /* Look up the schema OIDs and do permissions checks */
        ListCell* nspcell = NULL;

        foreach (nspcell, nspnames) {
            char* nspname = strVal(lfirst(nspcell));
            AclResult aclresult;

            /*
             * Note that we must do the permissions check against the target
             * role not the calling user.  We require CREATE privileges, since
             * without CREATE you won't be able to do anything using the
             * default privs anyway.
             */
            iacls->nspid = get_namespace_oid(nspname, false);

            aclresult = pg_namespace_aclcheck(iacls->nspid, iacls->roleid, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, nspname);

            SetDefaultACL(iacls);
        }
    }
}

/*
 * Create or update a pg_default_acl entry
 */
static void SetDefaultACL(InternalDefaultACL* iacls)
{
    AclMode this_privileges[PRIVS_ATTR_NUM];
    this_privileges[DML_PRIVS_INDEX] = iacls->privileges;
    this_privileges[DDL_PRIVS_INDEX] = iacls->ddl_privileges;
    char objtype;
    Relation rel = NULL;
    HeapTuple tuple = NULL;
    bool isNew = false;
    Acl* def_acl = NULL;
    Acl* old_acl = NULL;
    Acl* new_acl = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_default_acl];
    bool nulls[Natts_pg_default_acl] = {false};
    bool replaces[Natts_pg_default_acl] = {false};
    int noldmembers;
    int nnewmembers;
    Oid* oldmembers = NULL;
    Oid* newmembers = NULL;
    errno_t rc;

    rel = heap_open(DefaultAclRelationId, RowExclusiveLock);

    /*
     * The default for a global entry is the hard-wired default ACL for the
     * particular object type.	The default for non-global entries is an empty
     * ACL.  This must be so because global entries replace the hard-wired
     * defaults, while others are added on.
     */
    if (!OidIsValid(iacls->nspid))
        def_acl = acldefault(iacls->objtype, iacls->roleid);
    else
        def_acl = make_empty_acl();

    /*
     * Convert ACL object type to pg_default_acl object type and handle
     * all_privs option
     */
    switch (iacls->objtype) {
        case ACL_OBJECT_RELATION:
            objtype = DEFACLOBJ_RELATION;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_RELATION;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_RELATION;
            }
            break;

        case ACL_OBJECT_SEQUENCE:
            objtype = DEFACLOBJ_SEQUENCE;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_SEQUENCE;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_SEQUENCE;
            }
            break;

        case ACL_OBJECT_FUNCTION:
            objtype = DEFACLOBJ_FUNCTION;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_FUNCTION;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_FUNCTION;
            }
            break;
        case ACL_OBJECT_PACKAGE:
            objtype = DEFACLOBJ_FUNCTION;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_PACKAGE;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_PACKAGE;
            }
            break;
        case ACL_OBJECT_TYPE:
            objtype = DEFACLOBJ_TYPE;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_TYPE;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_TYPE;
            }
            break;
        case ACL_OBJECT_GLOBAL_SETTING:
            objtype = DEFACLOBJ_GLOBAL_SETTING;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_TYPE;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_KEY;
            }
            break;
        case ACL_OBJECT_COLUMN_SETTING:
            objtype = DEFACLOBJ_COLUMN_SETTING;
            if (iacls->all_privs && this_privileges[DML_PRIVS_INDEX] == ACL_NO_RIGHTS &&
                REMOVE_DDL_FLAG(this_privileges[DDL_PRIVS_INDEX]) == ACL_NO_RIGHTS) {
                this_privileges[DML_PRIVS_INDEX] = ACL_ALL_RIGHTS_TYPE;
                this_privileges[DDL_PRIVS_INDEX] = ACL_ALL_DDL_RIGHTS_KEY;
            }
            break;
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized objtype: %d", (int)iacls->objtype), errdetail("N/A"),
                    errcause("The object type is not supported for default privileges."),
                        erraction("Check ALTER DEFAULT PRIVILEGES syntax to obtain the supported object types.")));
            objtype = 0; /* keep compiler quiet */
            break;
    }

    /* Search for existing row for this object type in catalog */
    tuple = SearchSysCache3(
        DEFACLROLENSPOBJ, ObjectIdGetDatum(iacls->roleid), ObjectIdGetDatum(iacls->nspid), CharGetDatum(objtype));
    if (HeapTupleIsValid(tuple)) {
        Datum aclDatum;
        bool isNull = false;

        aclDatum = SysCacheGetAttr(DEFACLROLENSPOBJ, tuple, Anum_pg_default_acl_defaclacl, &isNull);
        if (!isNull)
            old_acl = DatumGetAclPCopy(aclDatum);
        else
            old_acl = NULL; /* this case shouldn't happen, probably */
        isNew = false;
    } else {
        old_acl = NULL;
        isNew = true;
    }

    if (old_acl != NULL) {
        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.  Collect data before
         * merge_acl_with_grant throws away old_acl.
         */
        noldmembers = aclmembers(old_acl, &oldmembers);
    } else {
        /* If no or null entry, start with the default ACL value */
        old_acl = aclcopy(def_acl);
        /* There are no old member roles according to the catalogs */
        noldmembers = 0;
        oldmembers = NULL;
    }

    /*
     * Generate new ACL.  Grantor of rights is always the same as the target
     * role.
     */
    new_acl = merge_acl_with_grant(old_acl,
        iacls->is_grant,
        iacls->grant_option,
        iacls->behavior,
        iacls->grantees,
        this_privileges,
        iacls->roleid,
        iacls->roleid);

    /*
     * If the result is the same as the default value, we do not need an
     * explicit pg_default_acl entry, and should in fact remove the entry if
     * it exists.  Must sort both arrays to compare properly.
     */
    aclitemsort(new_acl);
    aclitemsort(def_acl);
    if (aclequal(new_acl, def_acl)) {
        /* delete old entry, if indeed there is one */
        if (!isNew) {
            ObjectAddress myself;

            /*
             * The dependency machinery will take care of removing all
             * associated dependency entries.  We use DROP_RESTRICT since
             * there shouldn't be anything depending on this entry.
             */
            myself.classId = DefaultAclRelationId;
            myself.objectId = HeapTupleGetOid(tuple);
            myself.objectSubId = 0;

            performDeletion(&myself, DROP_RESTRICT, 0);
        }
    } else {
        /* Prepare to insert or update pg_default_acl entry */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        if (isNew) {
            /* insert new entry */
            values[Anum_pg_default_acl_defaclrole - 1] = ObjectIdGetDatum(iacls->roleid);
            values[Anum_pg_default_acl_defaclnamespace - 1] = ObjectIdGetDatum(iacls->nspid);
            values[Anum_pg_default_acl_defaclobjtype - 1] = CharGetDatum(objtype);
            values[Anum_pg_default_acl_defaclacl - 1] = PointerGetDatum(new_acl);

            newtuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
            (void)simple_heap_insert(rel, newtuple);
        } else {
            /* update existing entry */
            values[Anum_pg_default_acl_defaclacl - 1] = PointerGetDatum(new_acl);
            replaces[Anum_pg_default_acl_defaclacl - 1] = true;

            newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), values, nulls, replaces);
            simple_heap_update(rel, &newtuple->t_self, newtuple);
        }

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(rel, newtuple);

        /* these dependencies don't change in an update */
        if (isNew) {
            /* dependency on role */
            recordDependencyOnOwner(DefaultAclRelationId, HeapTupleGetOid(newtuple), iacls->roleid);

            /* dependency on namespace */
            if (OidIsValid(iacls->nspid)) {
                ObjectAddress myself, referenced;

                myself.classId = DefaultAclRelationId;
                myself.objectId = HeapTupleGetOid(newtuple);
                myself.objectSubId = 0;

                referenced.classId = NamespaceRelationId;
                referenced.objectId = iacls->nspid;
                referenced.objectSubId = 0;

                recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
            }
        }

        /*
         * Update the shared dependency ACL info
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        updateAclDependencies(DefaultAclRelationId,
            HeapTupleGetOid(newtuple),
            0,
            iacls->roleid,
            noldmembers,
            oldmembers,
            nnewmembers,
            newmembers);
    }

    if (HeapTupleIsValid(tuple))
        ReleaseSysCache(tuple);

    heap_close(rel, RowExclusiveLock);
}

/*
 * RemoveRoleFromObjectACL
 *
 * Used by shdepDropOwned to remove mentions of a role in ACLs
 */
void RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objid)
{
    if (classid == DefaultAclRelationId) {
        InternalDefaultACL iacls;
        Form_pg_default_acl pg_default_acl_tuple;
        Relation rel = NULL;
        ScanKeyData skey[1];
        SysScanDesc scan;
        HeapTuple tuple = NULL;

        /* first fetch info needed by SetDefaultACL */
        rel = heap_open(DefaultAclRelationId, AccessShareLock);

        ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));

        scan = systable_beginscan(rel, DefaultAclOidIndexId, true, NULL, 1, skey);

        tuple = systable_getnext(scan);

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("could not find tuple for default ACL %u", objid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        pg_default_acl_tuple = (Form_pg_default_acl)GETSTRUCT(tuple);

        iacls.roleid = pg_default_acl_tuple->defaclrole;
        iacls.nspid = pg_default_acl_tuple->defaclnamespace;

        switch (pg_default_acl_tuple->defaclobjtype) {
            case DEFACLOBJ_RELATION:
                iacls.objtype = ACL_OBJECT_RELATION;
                break;
            case DEFACLOBJ_SEQUENCE:
                iacls.objtype = ACL_OBJECT_SEQUENCE;
                break;
            case DEFACLOBJ_FUNCTION:
                iacls.objtype = ACL_OBJECT_FUNCTION;
                break;
            case DEFACLOBJ_PACKAGE:
                iacls.objtype = ACL_OBJECT_PACKAGE;
                break;
            case DEFACLOBJ_TYPE:
                iacls.objtype = ACL_OBJECT_TYPE;
                break;
            case DEFACLOBJ_GLOBAL_SETTING:
                iacls.objtype = ACL_OBJECT_GLOBAL_SETTING;
                break;
            case DEFACLOBJ_COLUMN_SETTING:
                iacls.objtype = ACL_OBJECT_COLUMN_SETTING;
                break;
            default:
                /* Shouldn't get here */
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("unexpected default ACL type: %d", (int)pg_default_acl_tuple->defaclobjtype),
                        errdetail("N/A"), errcause("The object type is not supported for default privilege."),
                            erraction("Check ALTER DEFAULT PRIVILEGES syntax to obtain the supported object types.")));
                break;
        }

        systable_endscan(scan);
        heap_close(rel, AccessShareLock);

        iacls.is_grant = false;
        iacls.all_privs = true;
        iacls.privileges = ACL_NO_RIGHTS;
        iacls.ddl_privileges = ACL_NO_DDL_RIGHTS;
        iacls.grantees = list_make1_oid(roleid);
        iacls.grant_option = false;
        iacls.behavior = DROP_CASCADE;

        /* Do it */
        SetDefaultACL(&iacls);
    } else {
        InternalGrant istmt;
        bool id_flag = false;

        for (size_t idx = 0; idx < sizeof(acl_class_id) / sizeof(acl_class_id[0]); idx++) {
            if (acl_class_id[idx].classid == classid) {
                istmt.objtype = acl_class_id[idx].objtype;
                id_flag = true;
                break;
            }
        }
        if (!id_flag) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("invalid object id"), errdetail("unexpected object class %u", classid),
                    errcause("The object type is not supported for GRANT/REVOKE."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
        }

        istmt.is_grant = false;
        istmt.objects = list_make1_oid(objid);
        istmt.all_privs = true;
        istmt.privileges = ACL_NO_RIGHTS;
        istmt.ddl_privileges = ACL_NO_DDL_RIGHTS;
        istmt.col_privs = NIL;
        istmt.col_ddl_privs = NIL;
        istmt.grantees = list_make1_oid(roleid);
        istmt.grant_option = false;
        istmt.behavior = DROP_CASCADE;

        ExecGrantStmt_oids(&istmt);
    }
}

/*
 * Remove a pg_default_acl entry
 */
void RemoveDefaultACLById(Oid defaclOid)
{
    Relation rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;

    rel = heap_open(DefaultAclRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(defaclOid));

    scan = systable_beginscan(rel, DefaultAclOidIndexId, true, NULL, 1, skey);

    tuple = systable_getnext(scan);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for default ACL %u", defaclOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    simple_heap_delete(rel, &tuple->t_self);
    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}

/*
 * expand_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each
 * specified attribute.  The per-column array is indexed starting at
 * FirstLowInvalidHeapAttributeNumber, up to relation's last attribute.
 */
static void expand_col_privileges(
    List* colnames, Oid table_oid, AclMode this_privileges, AclMode* col_privileges, int num_col_privileges)
{
    ListCell* cell = NULL;

    foreach (cell, colnames) {
        char* colname = strVal(lfirst(cell));
        AttrNumber attnum;

        attnum = get_attnum(table_oid, colname);
        if (attnum == InvalidAttrNumber) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("undefined column"),
                errdetail("column \"%s\" of relation \"%s\" does not exist", colname, get_rel_name(table_oid)),
                    errcause("The column of the relation does not exist."),
                        erraction("Check whether the column exists.")));
        }
        attnum -= FirstLowInvalidHeapAttributeNumber;
        if (attnum <= 0 || attnum >= num_col_privileges) { /* safety check */
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("column number out of range"), errdetail("column number \"%d\"out of range", attnum),
                    errcause("System error."), erraction("Contact engineer to support.")));
        }
        col_privileges[attnum] |= this_privileges;
    }
}

/*
 * expand_all_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each valid
 * attribute of a relation.  The per-column array is indexed starting at
 * FirstLowInvalidHeapAttributeNumber, up to relation's last attribute.
 */
static void expand_all_col_privileges(
    Oid table_oid, Form_pg_class classForm, AclMode this_privileges, AclMode* col_privileges, int num_col_privileges,  bool relhasbucket, bool relhasuids)
{
    AttrNumber curr_att;

    Assert(classForm->relnatts - FirstLowInvalidHeapAttributeNumber < num_col_privileges);
    for (curr_att = FirstLowInvalidHeapAttributeNumber + 1; curr_att <= classForm->relnatts; curr_att++) {
        HeapTuple attTuple = NULL;
        bool isdropped = false;

        if (curr_att == InvalidAttrNumber)
            continue;
        if (curr_att == UidAttributeNumber && !relhasuids) {
            continue;
        }
        /* Skip OID column if it doesn't exist */
        if (curr_att == ObjectIdAttributeNumber && !classForm->relhasoids)
            continue;

        if (curr_att == BucketIdAttributeNumber && !relhasbucket)
            continue;
        /* Views don't have any system columns at all */
        if ((classForm->relkind == RELKIND_VIEW || classForm->relkind == RELKIND_CONTQUERY) 
            && curr_att < 0)
            continue;

        attTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(table_oid), Int16GetDatum(curr_att));
        if (!HeapTupleIsValid(attTuple)) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u", curr_att, table_oid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        }

        isdropped = ((Form_pg_attribute)GETSTRUCT(attTuple))->attisdropped;

        ReleaseSysCache(attTuple);

        /* ignore dropped columns */
        if (isdropped)
            continue;

        col_privileges[curr_att - FirstLowInvalidHeapAttributeNumber] |= this_privileges;
    }
}

/*
 *	This processes attributes, but expects to be called from
 *	ExecGrant_Relation, not directly from ExecGrantStmt.
 */
static void ExecGrant_Attribute(InternalGrant* istmt, Oid relOid, const char* relname, AttrNumber attnum, Oid ownerId,
    AclMode col_privileges, AclMode col_ddl_privileges, Relation attRelation, const Acl* old_rel_acl)
{
    HeapTuple attr_tuple = NULL;
    Form_pg_attribute pg_attribute_tuple = NULL;
    Acl* old_acl = NULL;
    Acl* new_acl = NULL;
    Acl* merged_acl = NULL;
    Datum aclDatum;
    bool isNull = false;
    Oid grantorId;
    AclMode avail_goptions;
    AclMode avail_ddl_goptions;
    bool need_update = false;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_attribute];
    bool nulls[Natts_pg_attribute] = {false};
    bool replaces[Natts_pg_attribute] = {false};
    int noldmembers;
    int nnewmembers;
    Oid* oldmembers = NULL;
    Oid* newmembers = NULL;
    errno_t rc = EOK;

    attr_tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relOid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(attr_tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for attribute %d of relation %u", attnum, relOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    pg_attribute_tuple = (Form_pg_attribute)GETSTRUCT(attr_tuple);

    /*
     * Get working copy of existing ACL. If there's no ACL, substitute the
     * proper default.
     */
    aclDatum = SysCacheGetAttr(ATTNUM, attr_tuple, Anum_pg_attribute_attacl, &isNull);
    if (isNull) {
        old_acl = acldefault(ACL_OBJECT_COLUMN, ownerId);
        /* There are no old member roles according to the catalogs */
        noldmembers = 0;
        oldmembers = NULL;
    } else {
        old_acl = DatumGetAclPCopy(aclDatum);
        /* Get the roles mentioned in the existing ACL */
        noldmembers = aclmembers(old_acl, &oldmembers);
    }

    /*
     * In select_best_grantor we should consider existing table-level ACL bits
     * as well as the per-column ACL.  Build a new ACL that is their
     * concatenation.  (This is a bit cheap and dirty compared to merging them
     * properly with no duplications, but it's all we need here.)
     */
    merged_acl = aclconcat(old_rel_acl, old_acl);

    /* Determine ID to do the grant as, and available grant options */
    /* Need special treatment for relaions in schema dbe_perf and schema snapshot */
    Form_pg_class pg_class_tuple = NULL;
    HeapTuple class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(class_tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for relation %u", relOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    pg_class_tuple = (Form_pg_class)GETSTRUCT(class_tuple);
    Oid namespaceId = pg_class_tuple->relnamespace;
    ReleaseSysCache(class_tuple);

    select_best_grantor(GetUserId(), col_privileges, col_ddl_privileges, merged_acl, ownerId,
        &grantorId, &avail_goptions, &avail_ddl_goptions, IsMonitorSpace(namespaceId));

    /* Special treatment for opradmin in operation mode */
    if (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode) {
        grantorId = ownerId;
        avail_goptions = ACL_GRANT_OPTION_FOR(col_privileges);
        avail_ddl_goptions = ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(col_ddl_privileges));
    }

    pfree_ext(merged_acl);

    /*
     * Restrict the privileges to what we can actually grant, and emit the
     * standards-mandated warning and error messages.  Note: we don't track
     * whether the user actually used the ALL PRIVILEGES(columns) syntax for
     * each column; we just approximate it by whether all the possible
     * privileges are specified now.  Since the all_privs flag only determines
     * whether a warning is issued, this seems close enough.
     */
    bool all_privs = ((col_privileges == ACL_ALL_RIGHTS_COLUMN) && (col_ddl_privileges == ACL_ALL_DDL_RIGHTS_COLUMN));
    
    AclMode this_col_privileges[PRIVS_ATTR_NUM];
    restrict_and_check_grant(this_col_privileges, istmt->is_grant,
        avail_goptions,
        avail_ddl_goptions,
        all_privs,
        col_privileges,
        col_ddl_privileges,
        relOid,
        grantorId,
        ACL_KIND_COLUMN,
        relname,
        attnum,
        NameStr(pg_attribute_tuple->attname));

    /*
     * Generate new ACL.
     */
    new_acl = merge_acl_with_grant(old_acl,
        istmt->is_grant,
        istmt->grant_option,
        istmt->behavior,
        istmt->grantees,
        this_col_privileges,
        grantorId,
        ownerId);

    /*
     * We need the members of both old and new ACLs so we can correct the
     * shared dependency information.
     */
    nnewmembers = aclmembers(new_acl, &newmembers);

    /* finished building new ACL value, now insert it */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    /*
     * If the updated ACL is empty, we can set attacl to null, and maybe even
     * avoid an update of the pg_attribute row.  This is worth testing because
     * we'll come through here multiple times for any relation-level REVOKE,
     * even if there were never any column GRANTs.	Note we are assuming that
     * the "default" ACL state for columns is empty.
     */
    if (ACL_NUM(new_acl) > 0) {
        values[Anum_pg_attribute_attacl - 1] = PointerGetDatum(new_acl);
        need_update = true;
    } else {
        nulls[Anum_pg_attribute_attacl - 1] = true;
        need_update = !isNull;
    }
    replaces[Anum_pg_attribute_attacl - 1] = true;

    if (need_update) {
        newtuple = heap_modify_tuple(attr_tuple, RelationGetDescr(attRelation), values, nulls, replaces);

        simple_heap_update(attRelation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(attRelation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            RelationRelationId, relOid, attnum, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);
    }

    pfree_ext(new_acl);

    ReleaseSysCache(attr_tuple);
}

static void ExecGrantRelationTypeCheck(Form_pg_class classTuple, const InternalGrant* istmt,
    AclMode* privileges, AclMode* ddlPrivileges)
{
    /* Not sensible to grant on an index */
    if (classTuple->relkind == RELKIND_INDEX || classTuple->relkind == RELKIND_GLOBAL_INDEX)
        ereport(ERROR, (errmodule(MOD_SEC_FE), errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("unsupported object type"), errdetail("\"%s\" is an index", NameStr(classTuple->relname)),
                errcause("Index type is not supported for GRANT/REVOKE."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));

    /* Composite types aren't tables either */
    if (classTuple->relkind == RELKIND_COMPOSITE_TYPE)
        ereport(ERROR, (errmodule(MOD_SEC_FE), errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("unsupported object type"), errdetail("\"%s\" is a composite type", NameStr(classTuple->relname)),
                errcause("Composite type is not supported for GRANT/REVOKE."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));

    /* Used GRANT SEQUENCE on a non-sequence? */
    if (istmt->objtype == ACL_OBJECT_SEQUENCE && !RELKIND_IS_SEQUENCE(classTuple->relkind))
        ereport(ERROR, (errmodule(MOD_SEC_FE), errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("wrong object type"), errdetail("\"%s\" is not a sequence", NameStr(classTuple->relname)),
                errcause("GRANT/REVOKE SEQUENCE only support sequence objects."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));

    /* Adjust the default permissions based on object type */
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        if (RELKIND_IS_SEQUENCE(classTuple->relkind)) {
            *privileges = ACL_ALL_RIGHTS_SEQUENCE;
            *ddlPrivileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
                ACL_ALL_DDL_RIGHTS_SEQUENCE : ACL_NO_DDL_RIGHTS;
        } else {
            *privileges = ACL_ALL_RIGHTS_RELATION;
            *ddlPrivileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
                ACL_ALL_DDL_RIGHTS_RELATION : ACL_NO_DDL_RIGHTS;
        }
    } else {
        *privileges = istmt->privileges;
        *ddlPrivileges = istmt->ddl_privileges;
    }
}

static void ExecGrantRelationPrivilegesCheck(Form_pg_class tuple, AclMode* privileges, AclMode* ddlprivileges)
{
    if (RELKIND_IS_SEQUENCE(tuple->relkind)) {
        /*
            * For backward compatibility, just throw a warning for
            * invalid sequence permissions when using the non-sequence
            * GRANT syntax.
            */
        if ((*ddlprivileges & ~((AclMode)ACL_ALL_DDL_RIGHTS_SEQUENCE)) ||
            (*privileges & ~((AclMode)ACL_ALL_RIGHTS_SEQUENCE))) {
            /*
                * Mention the object name because the user needs to know
                * which operations succeeded.	This is required because
                * WARNING allows the command to continue.
                */
            ereport(WARNING, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("sequence \"%s\" only supports USAGE, SELECT, UPDATE, "
                        "ALTER, DROP and COMMENT privileges", NameStr(tuple->relname))));
            *ddlprivileges &= (AclMode)ACL_ALL_DDL_RIGHTS_SEQUENCE;
            *privileges &= (AclMode)ACL_ALL_RIGHTS_SEQUENCE;
        }
    } else {
        if (*privileges & ~((AclMode)ACL_ALL_RIGHTS_RELATION)) {
            /*
                * USAGE is the only permission supported by sequences but
                * not by non-sequences.  Don't mention the object name
                * because we didn't in the combined TABLE | SEQUENCE
                * check.
                */
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("invalid privilege type USAGE for table"), errdetail("N/A"),
                    errcause("GRANT/REVOKE TABLE do not support USAGE privilege."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types for tables.")));
        }
    }
}

/*
 *	This processes both sequences and non-sequences.
 */
void ExecGrant_Relation(InternalGrant* istmt)
{
    Relation relation = NULL;
    Relation attRelation = NULL;
    ListCell* cell = NULL;

    relation = heap_open(RelationRelationId, RowExclusiveLock);
    attRelation = heap_open(AttributeRelationId, RowExclusiveLock);
    foreach (cell, istmt->objects) {
        Oid relOid = lfirst_oid(cell);
        Datum aclDatum;
        Form_pg_class pg_class_tuple = NULL;
        bool isNull = false;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        AclMode privileges, ddl_privileges;
        AclMode* col_privileges = NULL;
        AclMode* col_ddl_privileges = NULL;
        int num_col_privileges;
        bool have_col_privileges = false;
        Acl* old_acl = NULL;
        Acl* old_rel_acl = NULL;
        int noldmembers;
        Oid* oldmembers = NULL;
        Oid ownerId;
        HeapTuple tuple = NULL;
        ListCell* cell_colprivs = NULL;

        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", relOid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        pg_class_tuple = (Form_pg_class)GETSTRUCT(tuple);

        ExecGrantRelationTypeCheck(pg_class_tuple, istmt, &privileges, &ddl_privileges);

        /*
         * The GRANT TABLE syntax can be used for sequences and non-sequences,
         * so we have to look at the relkind to determine the supported
         * permissions.  The OR of table and sequence permissions were already
         * checked.
         */
        if (istmt->objtype == ACL_OBJECT_RELATION) {
            ExecGrantRelationPrivilegesCheck(pg_class_tuple, &privileges, &ddl_privileges);
        }

        /*
         * Set up array in which we'll accumulate any column privilege bits
         * that need modification.	The array is indexed such that entry [0]
         * corresponds to FirstLowInvalidHeapAttributeNumber.
         */
        CatalogRelationBuildParam catalogParam = GetCatalogParam(relOid);
        if (catalogParam.oid != InvalidOid)
            pg_class_tuple->relnatts = catalogParam.natts;
        num_col_privileges = pg_class_tuple->relnatts - FirstLowInvalidHeapAttributeNumber + 1;
        col_privileges = (AclMode*)palloc0(num_col_privileges * sizeof(AclMode));
        col_ddl_privileges = (AclMode*)palloc0(num_col_privileges * sizeof(AclMode));
        have_col_privileges = false;

        /*
         * If we are revoking relation privileges that are also column
         * privileges, we must implicitly revoke them from each column too,
         * per SQL spec.  (We don't need to implicitly add column privileges
         * during GRANT because the permissions-checking code always checks
         * both relation and per-column privileges.)
         */
        if (!istmt->is_grant && ((privileges & ACL_ALL_RIGHTS_COLUMN) != 0 ||
            (REMOVE_DDL_FLAG(ddl_privileges) & ACL_ALL_DDL_RIGHTS_COLUMN) != 0)) {
            bool  hasbucket = false;
            bool  relhasuids = false;
            bool  isNull = false;
            Datum datum;

            datum = heap_getattr(tuple, Anum_pg_class_relbucket, GetDefaultPgClassDesc(), &isNull);
            if (!isNull && DatumGetObjectId(datum) != VirtualSegmentOid) {
                Assert(OidIsValid(DatumGetObjectId(datum)));
                hasbucket = true;
            }
            if (pg_class_tuple->relkind == RELKIND_RELATION) {
                bytea* options = extractRelOptions(tuple, GetDefaultPgClassDesc(), InvalidOid);
                relhasuids = StdRdOptionsHasUids(options, pg_class_tuple->relkind);
                pfree_ext(options);
            }
            // get if relhasuids
            expand_all_col_privileges(relOid, pg_class_tuple, privileges & ACL_ALL_RIGHTS_COLUMN,
                col_privileges, num_col_privileges, hasbucket, relhasuids);
            expand_all_col_privileges(relOid, pg_class_tuple, ddl_privileges & ACL_ALL_DDL_RIGHTS_COLUMN,
                col_ddl_privileges, num_col_privileges, hasbucket, relhasuids);
            have_col_privileges = true;
        }

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_class_tuple->relowner;
        aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &isNull);
        if (isNull) {
            switch (pg_class_tuple->relkind) {
                case RELKIND_SEQUENCE:
                case RELKIND_LARGE_SEQUENCE:
                    old_acl = acldefault(ACL_OBJECT_SEQUENCE, ownerId);
                    break;
                default:
                    old_acl = acldefault(ACL_OBJECT_RELATION, ownerId);
                    break;
            }
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Need an extra copy of original rel ACL for column handling */
        old_rel_acl = aclcopy(old_acl);

        /*
         * Handle relation-level privileges, if any were specified
         */
        if (privileges != ACL_NO_RIGHTS || REMOVE_DDL_FLAG(ddl_privileges) != ACL_NO_RIGHTS) {
            AclMode avail_goptions, avail_ddl_goptions;
            Acl* new_acl = NULL;
            Oid grantorId;
            HeapTuple newtuple = NULL;
            Datum values[Natts_pg_class];
            bool nulls[Natts_pg_class] = {false};
            bool replaces[Natts_pg_class] = {false};
            int nnewmembers;
            Oid* newmembers = NULL;
            AclObjectKind aclkind;

            /* Determine ID to do the grant as, and available grant options */
            /* Need special treatment for relations in schema dbe_perf and schema snapshot */
            Oid namespaceId = pg_class_tuple->relnamespace;
            select_best_grantor(GetUserId(), privileges, ddl_privileges, old_acl, ownerId,
                &grantorId, &avail_goptions, &avail_ddl_goptions, IsMonitorSpace(namespaceId));

            /* Special treatment for opradmin in operation mode */
            if (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode) {
                grantorId = ownerId;
                avail_goptions = ACL_GRANT_OPTION_FOR(privileges);
                avail_ddl_goptions = ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ddl_privileges));
            }

            switch (pg_class_tuple->relkind) {
                case RELKIND_SEQUENCE:
                case RELKIND_LARGE_SEQUENCE:
                    aclkind = ACL_KIND_SEQUENCE;
                    break;
                default:
                    aclkind = ACL_KIND_CLASS;
                    break;
            }

            /*
             * Restrict the privileges to what we can actually grant, and emit
             * the standards-mandated warning and error messages.
             */
            restrict_and_check_grant(this_privileges, istmt->is_grant,
                avail_goptions, avail_ddl_goptions, istmt->all_privs, privileges, ddl_privileges, relOid, grantorId,
                aclkind, NameStr(pg_class_tuple->relname), 0, NULL);

            /*
             * Generate new ACL.
             */
            new_acl = merge_acl_with_grant(old_acl,
                istmt->is_grant, istmt->grant_option, istmt->behavior, istmt->grantees,
                this_privileges, grantorId, ownerId);

            /*
             * We need the members of both old and new ACLs so we can correct
             * the shared dependency information.
             */
            nnewmembers = aclmembers(new_acl, &newmembers);

            /* finished building new ACL value, now insert it */
            errno_t rc = EOK;
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "\0", "\0");
            rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "\0", "\0");
            rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
            securec_check(rc, "\0", "\0");

            replaces[Anum_pg_class_relacl - 1] = true;
            values[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);

            newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

            simple_heap_update(relation, &newtuple->t_self, newtuple);

            /* keep the catalog indexes up to date */
            CatalogUpdateIndexes(relation, newtuple);

            /* Update the shared dependency ACL info */
            updateAclDependencies(
                RelationRelationId, relOid, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

            /* Recode time of grant relation. */
            recordRelationMTime(relOid, pg_class_tuple->relkind);

            pfree_ext(new_acl);
        }

        /*
         * Handle column-level privileges, if any were specified or implied.
         * We first expand the user-specified column privileges into the
         * array, and then iterate over all nonempty array entries.
         */
        foreach (cell_colprivs, istmt->col_privs) {
            AccessPriv* col_privs = (AccessPriv*)lfirst(cell_colprivs);

            if (col_privs->priv_name == NULL)
                privileges = ACL_ALL_RIGHTS_COLUMN;
            else
                privileges = string_to_privilege(col_privs->priv_name);

            if (privileges & ~((AclMode)ACL_ALL_RIGHTS_COLUMN))
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid privilege type %s for column", privilege_to_string(privileges)), errdetail("N/A"),
                        errcause("The privilege type is not supported for column object."),
                            erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types "
                                "for column object.")));

            if (RELKIND_IS_SEQUENCE(pg_class_tuple->relkind) && (privileges & ~((AclMode)ACL_SELECT))) {
                /*
                 * The only column privilege allowed on sequences is SELECT.
                 * This is a warning not error because we do it that way for
                 * relation-level privileges.
                 */
                ereport(WARNING, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg("sequence \"%s\" only supports SELECT column privileges",
                            NameStr(pg_class_tuple->relname))));

                privileges &= (AclMode)ACL_SELECT;
            }

            expand_col_privileges(col_privs->cols, relOid, privileges, col_privileges, num_col_privileges);
            have_col_privileges = true;
        }

        foreach (cell_colprivs, istmt->col_ddl_privs) {
            AccessPriv* col_privs = (AccessPriv*)lfirst(cell_colprivs);

            if (col_privs->priv_name == NULL)
                ddl_privileges = ACL_ALL_DDL_RIGHTS_COLUMN;
            else
                ddl_privileges = string_to_privilege(col_privs->priv_name);

            if (ddl_privileges & ~((AclMode)ACL_ALL_DDL_RIGHTS_COLUMN))
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("invalid privilege type %s for column", privilege_to_string(ddl_privileges)),
                        errdetail("N/A"), errcause("The privilege type is not supported for column object."),
                            erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types "
                                "for column object.")));

            expand_col_privileges(col_privs->cols, relOid, ddl_privileges, col_ddl_privileges, num_col_privileges);
            have_col_privileges = true;
        }

        if (have_col_privileges) {
            AttrNumber i;

            for (i = 0; i < num_col_privileges; i++) {
                if (col_privileges[i] == ACL_NO_RIGHTS && REMOVE_DDL_FLAG(col_ddl_privileges[i]) == ACL_NO_RIGHTS)
                    continue;
                ExecGrant_Attribute(istmt, relOid, NameStr(pg_class_tuple->relname),
                    i + FirstLowInvalidHeapAttributeNumber, ownerId, col_privileges[i], col_ddl_privileges[i],
                    attRelation, old_rel_acl);
            }
        }

        pfree_ext(old_rel_acl);
        pfree_ext(col_privileges);
        pfree_ext(col_ddl_privileges);

        ReleaseSysCache(tuple);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(attRelation, RowExclusiveLock);
    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Database(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_DATABASE;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_DATABASE : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(DatabaseRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid datId = lfirst_oid(cell);
        Form_pg_database pg_database_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_database];
        bool nulls[Natts_pg_database] = {false};
        bool replaces[Natts_pg_database] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        HeapTuple tuple = NULL;

        tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(datId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for database %u", datId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_database_tuple = (Form_pg_database)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_database_tuple->datdba;
        aclDatum = heap_getattr(tuple, Anum_pg_database_datacl, RelationGetDescr(relation), &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            datId,
            grantorId,
            ACL_KIND_DATABASE,
            NameStr(pg_database_tuple->datname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "", "");
        rc = memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
        securec_check(rc, "", "");

        replaces[Anum_pg_database_datacl - 1] = true;
        values[Anum_pg_database_datacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            DatabaseRelationId, HeapTupleGetOid(tuple), 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Fdw(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;
    bool is_gc_fdw = false;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges =  ACL_ALL_RIGHTS_FDW;
        istmt->ddl_privileges =  ACL_ALL_DDL_RIGHTS_FDW;
    }

    relation = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid fdwid = lfirst_oid(cell);
        Form_pg_foreign_data_wrapper pg_fdw_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_foreign_data_wrapper];
        bool nulls[Natts_pg_foreign_data_wrapper] = {false};
        bool replaces[Natts_pg_foreign_data_wrapper] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;

        tuple = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for foreign-data wrapper %u", fdwid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_fdw_tuple = (Form_pg_foreign_data_wrapper)GETSTRUCT(tuple);

        if (pg_strcasecmp(pg_fdw_tuple->fdwname.data, GC_FDW) == 0)
            is_gc_fdw = true;

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_fdw_tuple->fdwowner;
        aclDatum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tuple, Anum_pg_foreign_data_wrapper_fdwacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_FDW, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            fdwid,
            grantorId,
            ACL_KIND_FDW,
            NameStr(pg_fdw_tuple->fdwname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;
        values[Anum_pg_foreign_data_wrapper_fdwacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(ForeignDataWrapperRelationId,
            HeapTupleGetOid(tuple),
            0,
            ownerId,
            noldmembers,
            oldmembers,
            nnewmembers,
            newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);

    /* enable rls(row level security) for pg_foreign_server when grant gc_fdw to normal user. */
    if (is_gc_fdw) {
        Oid rlsPolicyId = get_rlspolicy_oid(ForeignServerRelationId, "pg_foreign_server_rls", true);
        if (OidIsValid(rlsPolicyId) == false) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_foreign_server", "pg_foreign_server_rls", "has_server_privilege", "oid", "usage");
            Relation rel = relation_open(ForeignServerRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(rel, ShareUpdateExclusiveLock);
        }
    }
}

static void ExecGrant_ForeignServer(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_FOREIGN_SERVER : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(ForeignServerRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid srvid = lfirst_oid(cell);
        Form_pg_foreign_server pg_server_tuple;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_foreign_server];
        bool nulls[Natts_pg_foreign_server] = {false};
        bool replaces[Natts_pg_foreign_server] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;

        tuple = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(srvid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for foreign server %u", srvid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_server_tuple = (Form_pg_foreign_server)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_server_tuple->srvowner;
        aclDatum = SysCacheGetAttr(FOREIGNSERVEROID, tuple, Anum_pg_foreign_server_srvacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            srvid,
            grantorId,
            ACL_KIND_FOREIGN_SERVER,
            NameStr(pg_server_tuple->srvname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_foreign_server_srvacl - 1] = true;
        values[Anum_pg_foreign_server_srvacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(ForeignServerRelationId,
            HeapTupleGetOid(tuple),
            0,
            ownerId,
            noldmembers,
            oldmembers,
            nnewmembers,
            newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Function(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_FUNCTION;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_FUNCTION : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(ProcedureRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid funcId = lfirst_oid(cell);
        Form_pg_proc pg_proc_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_proc];
        bool nulls[Natts_pg_proc] = {false};
        bool replaces[Natts_pg_proc] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u", funcId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_proc_tuple = (Form_pg_proc)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_proc_tuple->proowner;
        aclDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        /* Need special treatment for procs in schema dbe_perf and schema snapshot */
        Oid namespaceId = pg_proc_tuple->pronamespace;
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions, IsMonitorSpace(namespaceId));

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            funcId,
            grantorId,
            ACL_KIND_PROC,
            NameStr(pg_proc_tuple->proname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_proc_proacl - 1] = true;
        values[Anum_pg_proc_proacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            ProcedureRelationId, funcId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* Recode time of grant function. */
        UpdatePgObjectMtime(funcId, OBJECT_TYPE_PROC);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Language(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_LANGUAGE;
        istmt->ddl_privileges = ACL_ALL_DDL_RIGHTS_LANGUAGE;
    }

    relation = heap_open(LanguageRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid langId = lfirst_oid(cell);
        Form_pg_language pg_language_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_language];
        bool nulls[Natts_pg_language] = {false};
        bool replaces[Natts_pg_language] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;

        tuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(langId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for language %u", langId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_language_tuple = (Form_pg_language)GETSTRUCT(tuple);

        /*
         * For untrust "java" and "internal" language,
         * only system admin can use "java" and "internal" languages.
         * For untrust "c" language,
         * both system admin and common users can use "c" languages.
         */
        if (!pg_language_tuple->lanpltrusted && ClanguageId != langId)
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("Grant/revoke on untrusted languages if forbidden."),
                    errdetail("language \"%s\" is not trusted", NameStr(pg_language_tuple->lanname)),
                        errcause("Grant/revoke on untrusted languages if forbidden."),
                            erraction("Support grant/revoke on trusted C languages")));

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_language_tuple->lanowner;
        aclDatum = SysCacheGetAttr(LANGNAME, tuple, Anum_pg_language_lanacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId, langId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            langId,
            grantorId,
            ACL_KIND_LANGUAGE,
            NameStr(pg_language_tuple->lanname),
            0,
            NULL);

        /* For language c, prevent uncontrolled grant permission. */
        if (ClanguageId == langId && istmt->grant_option)
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Forbid grant language c to user with grant option."),
                    errdetail("Only support grant language c to user."),
                        errcause("Forbid grant language c to user with grant option."),
                            erraction("Only support grant language c to user.")));

        if (ClanguageId == langId && list_member_oid(istmt->grantees, ACL_ID_PUBLIC))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Forbid grant language c to public."),
                    errdetail("Only support grant language c to specified users."),
                        errcause("Forbid grant language c to public."),
                            erraction("Grant language c to specified users.")));

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_language_lanacl - 1] = true;
        values[Anum_pg_language_lanacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            LanguageRelationId, HeapTupleGetOid(tuple), 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Package(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_PACKAGE;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_FUNCTION : ACL_NO_DDL_RIGHTS;
    }
    relation = heap_open(PackageRelationId, RowExclusiveLock);
    foreach (cell, istmt->objects) {
        Oid pkgId = lfirst_oid(cell);
        Form_gs_package gs_package_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_gs_package];
        bool nulls[Natts_gs_package] = {false};
        bool replaces[Natts_gs_package] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;
        tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgId));
        if (!HeapTupleIsValid(tuple))
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for package %u", pkgId)));
        gs_package_tuple = (Form_gs_package)GETSTRUCT(tuple);
        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = gs_package_tuple->pkgowner;
        aclDatum = SysCacheGetAttr(PACKAGEOID, tuple, Anum_gs_package_pkgacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_PACKAGE, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }
        /* Determine ID to do the grant as, and available grant options */
        /* Need special treatment for procs in schema dbe_perf */
        Oid namespaceId = gs_package_tuple->pkgnamespace;
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions, IsPerformanceNamespace(namespaceId));
        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            pkgId,
            grantorId,
            ACL_KIND_PACKAGE,
            NameStr(gs_package_tuple->pkgname),
            0,
            NULL);
        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);
        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);
        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");
        replaces[Anum_gs_package_pkgacl - 1] = true;
        values[Anum_gs_package_pkgacl - 1] = PointerGetDatum(new_acl);
        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &newtuple->t_self, newtuple);
        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);
        /* Update the shared dependency ACL info */
        updateAclDependencies(
            PackageRelationId, pkgId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);
        ReleaseSysCache(tuple);
        pfree_ext(new_acl);
        /* Recode time of grant function. */
        UpdatePgObjectMtime(pkgId, OBJECT_TYPE_PKGSPEC);
        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }
    heap_close(relation, RowExclusiveLock);
}
static void ExecGrant_Largeobject(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
        istmt->ddl_privileges = ACL_ALL_DDL_RIGHTS_LARGEOBJECT;
    }

    relation = heap_open(LargeObjectMetadataRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid loid = lfirst_oid(cell);
        Form_pg_largeobject_metadata form_lo_meta = NULL;
        char loname[NAMEDATALEN] = {0};
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_largeobject_metadata];
        bool nulls[Natts_pg_largeobject_metadata] = {false};
        bool replaces[Natts_pg_largeobject_metadata] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        ScanKeyData entry[1];
        SysScanDesc scan;
        HeapTuple tuple = NULL;
        errno_t rc = EOK;

        /* There's no syscache for pg_largeobject_metadata */
        ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));

        scan = systable_beginscan(relation, LargeObjectMetadataOidIndexId, true, NULL, 1, entry);

        tuple = systable_getnext(scan);
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for large object %u", loid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        form_lo_meta = (Form_pg_largeobject_metadata)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = form_lo_meta->lomowner;
        aclDatum = heap_getattr(tuple, Anum_pg_largeobject_metadata_lomacl, RelationGetDescr(relation), &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_LARGEOBJECT, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        rc = snprintf_s(loname, sizeof(loname), sizeof(loname) - 1, "large object %u", loid);
        securec_check_ss(rc, "", "");
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            loid,
            grantorId,
            ACL_KIND_LARGEOBJECT,
            loname,
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_largeobject_metadata_lomacl - 1] = true;
        values[Anum_pg_largeobject_metadata_lomacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(LargeObjectRelationId,
            HeapTupleGetOid(tuple),
            0,
            ownerId,
            noldmembers,
            oldmembers,
            nnewmembers,
            newmembers);

        systable_endscan(scan);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Namespace(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_NAMESPACE;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_NAMESPACE : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(NamespaceRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid nspid = lfirst_oid(cell);
        Form_pg_namespace pg_namespace_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_namespace];
        bool nulls[Natts_pg_namespace] = {false};
        bool replaces[Natts_pg_namespace] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t rc = EOK;

        tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for namespace %u", nspid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_namespace_tuple = (Form_pg_namespace)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_namespace_tuple->nspowner;
        aclDatum = SysCacheGetAttr(NAMESPACENAME, tuple, Anum_pg_namespace_nspacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /*
         * Determine ID to do the grant as, and available grant options.
         * Need special treatment for schema dbe_perf, snapshot and pg_catalog.
         */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions, IsMonitorSpace(nspid), IsSystemNamespace(nspid));

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            nspid,
            grantorId,
            ACL_KIND_NAMESPACE,
            NameStr(pg_namespace_tuple->nspname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_namespace_nspacl - 1] = true;
        values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            NamespaceRelationId, HeapTupleGetOid(tuple), 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

/*
 * @Description : Set nodegroup all privileges
 * @in grantStmt - grantstmt
 * @return - void
 */
static void set_nodegroup_all_privileges(InternalGrant* grantStmt) {
    if (grantStmt->all_privs && grantStmt->privileges == ACL_NO_RIGHTS &&
        grantStmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        grantStmt->privileges = ACL_ALL_RIGHTS_NODEGROUP;
        grantStmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_NODEGROUP : ACL_NO_DDL_RIGHTS;
    }    
}

/*
 * Check whether the node group is logic cluster and check virtual cluster CREATE privilege can be granted.
 * return - void
 */
static void check_vc_create_priv(Oid groupId, const FormData_pgxc_group *pgxc_group_tuple, const InternalGrant *grantStmt)
{
    if (is_logic_cluster(groupId)) {
        if (!superuser() && get_current_lcgroup_oid() != groupId) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Role %s has not privilege to grant/revoke node group %s.", GetUserNameFromId(GetUserId()),
                    NameStr(pgxc_group_tuple->group_name)), errdetail("Must have sysadmin privilege."),
                        errcause("Role has not privilege to grant/revoke node group."),
                            erraction("Must have sysadmin privilege.")));
        }

        /* Check if virtual cluster CREATE privilege can be granted. */
        /* In restore mode, don't need to check. */
        if (!isRestoreMode && grantStmt->is_grant && (grantStmt->all_privs || (grantStmt->privileges & ACL_CREATE))) {
            foreach_cell(granteeCell, grantStmt->grantees)
            {
                Oid roleid = lfirst_oid(granteeCell);
                if (!superuser_arg(roleid) && !systemDBA_arg(roleid)) {
                    Oid group_oid = get_pgxc_logic_groupoid(roleid);
                    if (group_oid != groupId) {
                        if (OidIsValid(group_oid)) {
                            char in_redis = get_pgxc_group_redistributionstatus(group_oid);
                            if (in_redis == PGXC_REDISTRIBUTION_SRC_GROUP) {
                                if (groupId == PgxcGroupGetRedistDestGroupOid())
                                    continue;
                            }
                        }
                        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            (errmsg("Can not grant CREATE privilege on node group %u to role %u in node group %u.",
                                groupId, roleid, group_oid), errdetail("Must have sysadmin privilege."),
                                    errcause("Role has not privilege to grant CREATE privilege node group."),
                                        erraction("Must have sysadmin privilege."))));
                    }
                }
            }
        }
    }
}

/*
 * @Description :  Do actual node group privilege grant
 * @in grantStmt - grantstmt
 * @return - void
 */
static void ExecGrant_NodeGroup(InternalGrant* grantStmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    /* set nodegroup all privileges */
    set_nodegroup_all_privileges(grantStmt); 

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    foreach (cell, grantStmt->objects) {
        Oid groupId = lfirst_oid(cell);
        Form_pgxc_group pgxc_group_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pgxc_group];
        bool nulls[Natts_pgxc_group] = {false};
        bool replaces[Natts_pgxc_group] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        HeapTuple tuple = NULL;
        errno_t rc = EOK;

        /* Search syscache for pgxc_group */
        tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupId));
        if (!HeapTupleIsValid(tuple)) {
            ereport(WARNING, (errmsg("cache lookup failed for node group with oid %u", groupId)));
            continue;
        }

        pgxc_group_tuple = (Form_pgxc_group)GETSTRUCT(tuple);

        /* Check whether the node group is logic cluster and virtual cluster CREATE privilege can be granted. */
        check_vc_create_priv(groupId, pgxc_group_tuple, grantStmt);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = BOOTSTRAP_SUPERUSERID;

        isNull = true;

        aclDatum = heap_getattr(tuple, Anum_pgxc_group_group_acl, RelationGetDescr(relation), &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_NODEGROUP, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        if (is_lcgroup_admin()) {
            select_best_grantor(ownerId, grantStmt->privileges, grantStmt->ddl_privileges, old_acl, ownerId,
                &grantorId, &avail_goptions, &avail_ddl_goptions);
        } else {
            select_best_grantor(GetUserId(), grantStmt->privileges, grantStmt->ddl_privileges, old_acl, ownerId,
                &grantorId, &avail_goptions, &avail_ddl_goptions);
            if (have_createdb_privilege()) {
                grantorId = ownerId;
                avail_goptions = ACL_GRANT_OPTION_FOR(grantStmt->privileges);
            }
        }

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, grantStmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            grantStmt->all_privs,
            grantStmt->privileges,
            grantStmt->ddl_privileges,
            groupId,
            grantorId,
            ACL_KIND_NODEGROUP,
            NameStr(pgxc_group_tuple->group_name),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            grantStmt->is_grant,
            grantStmt->grant_option,
            grantStmt->behavior,
            grantStmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, Natts_pgxc_group * sizeof(Datum), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, Natts_pgxc_group * sizeof(bool), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, Natts_pgxc_group * sizeof(bool), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pgxc_group_group_acl - 1] = true;
        values[Anum_pgxc_group_group_acl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            PgxcGroupRelationId, groupId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

void grantNodeGroupToRole(Oid group_id, Oid roleid, AclMode privileges, bool is_grant)
{
    InternalGrant istmt;

    if (!IS_PGXC_COORDINATOR)
        return;

    istmt.is_grant = is_grant;
    istmt.grantees = NIL;
    istmt.objects = NIL;
    istmt.grant_option = false;
    istmt.all_privs = false;
    istmt.privileges = privileges;
    istmt.ddl_privileges = ACL_NO_DDL_RIGHTS;
    istmt.objtype = ACL_OBJECT_NODEGROUP;
    istmt.col_privs = NIL;
    istmt.col_ddl_privs = NIL;
    istmt.behavior = DROP_CASCADE;
    istmt.grantees = lappend_oid(istmt.grantees, roleid);
    istmt.objects = lappend_oid(istmt.objects, group_id);
    ExecGrant_NodeGroup(&istmt);
}

Acl* getAclNodeGroup()
{
    InternalGrant grantStmt;
    grantStmt.all_privs = false;
    grantStmt.privileges = ACL_ALL_RIGHTS_NODEGROUP;
    grantStmt.ddl_privileges = ACL_NO_DDL_RIGHTS;
    grantStmt.is_grant = true;
    grantStmt.grantees = NIL;
    Acl* old_acl = NULL;
    Acl* new_acl = NULL;
    AclMode avail_goptions;
    AclMode avail_ddl_goptions;
    AclMode this_privileges[PRIVS_ATTR_NUM];
    Oid grantorId;

    Oid ownerId = BOOTSTRAP_SUPERUSERID;
    old_acl = acldefault(ACL_OBJECT_NODEGROUP, ownerId);
    grantStmt.grantees = lappend_oid(grantStmt.grantees, ACL_ID_PUBLIC);

    /* Determine ID to do the grant as, and available grant options */
    select_best_grantor(GetUserId(), grantStmt.privileges, grantStmt.ddl_privileges, old_acl, ownerId,
        &grantorId, &avail_goptions, &avail_ddl_goptions);
    if (have_createdb_privilege()) {
        grantorId = ownerId;
        avail_goptions = ACL_GRANT_OPTION_FOR(grantStmt.privileges);
    }

    /*
     * Restrict the privileges to what we can actually grant, and emit the
     * standards-mandated warning and error messages.
     */
    restrict_and_check_grant(this_privileges, grantStmt.is_grant,
        avail_goptions,
        avail_ddl_goptions,
        grantStmt.all_privs,
        grantStmt.privileges,
        grantStmt.ddl_privileges,
        0,
        grantorId,
        ACL_KIND_NODEGROUP,
        NULL,
        0,
        NULL);

    /*
     * Generate new ACL.
     */
    new_acl = merge_acl_with_grant(
        old_acl, grantStmt.is_grant, false, DROP_RESTRICT, grantStmt.grantees, this_privileges, grantorId, ownerId);
    return new_acl;
}

static void ExecGrant_Tablespace(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_TABLESPACE;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_TABLESPACE : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(TableSpaceRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid tblId = lfirst_oid(cell);
        Form_pg_tablespace pg_tablespace_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_tablespace];
        bool nulls[Natts_pg_tablespace] = {false};
        bool replaces[Natts_pg_tablespace] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        HeapTuple tuple = NULL;
        errno_t rc = EOK;

        /* Search syscache for pg_tablespace */
        tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(tblId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for tablespace %u", tblId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_tablespace_tuple = (Form_pg_tablespace)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_tablespace_tuple->spcowner;
        aclDatum = heap_getattr(tuple, Anum_pg_tablespace_spcacl, RelationGetDescr(relation), &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            tblId,
            grantorId,
            ACL_KIND_TABLESPACE,
            NameStr(pg_tablespace_tuple->spcname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_tablespace_spcacl - 1] = true;
        values[Anum_pg_tablespace_spcacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            TableSpaceRelationId, tblId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Type(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_TYPE;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_TYPE : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(TypeRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid typId = lfirst_oid(cell);
        Form_pg_type pg_type_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_type];
        bool nulls[Natts_pg_type] = {false};
        bool replaces[Natts_pg_type] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        HeapTuple tuple = NULL;
        errno_t rc = EOK;

        /* Search syscache for pg_type */
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typId));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u", typId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        pg_type_tuple = (Form_pg_type)GETSTRUCT(tuple);

        if (pg_type_tuple->typelem != 0 && pg_type_tuple->typlen == -1)
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("cannot set privileges of array types"),
                    errdetail("Set the privileges of the element type instead."),
                        errcause("Cannot set privileges of array types."),
                            erraction("Set the privileges of the element type instead.")));

        /* Used GRANT DOMAIN on a non-domain? */
        if (istmt->objtype == ACL_OBJECT_DOMAIN && pg_type_tuple->typtype != TYPTYPE_DOMAIN)
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("wrong object type"), errdetail("\"%s\" is not a domain", NameStr(pg_type_tuple->typname)),
                    errcause("GRANT/REVOKE DOMAIN only support domain objects."),
                    erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_type_tuple->typowner;
        aclDatum = heap_getattr(tuple, Anum_pg_type_typacl, RelationGetDescr(relation), &isNull);
        if (isNull) {
            old_acl = acldefault(istmt->objtype, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            typId,
            grantorId,
            ACL_KIND_TYPE,
            NameStr(pg_type_tuple->typname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_type_typacl - 1] = true;
        values[Anum_pg_type_typacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(TypeRelationId, typId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

/*
 * ExecGrant_DataSource:
 * 	grant privileges on given data source to specific roles
 */
static void ExecGrant_DataSource(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_DATA_SOURCE;
        istmt->ddl_privileges = ACL_ALL_DDL_RIGHTS_DATA_SOURCE;
    }

    relation = heap_open(DataSourceRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid srcid = lfirst_oid(cell);
        Form_pg_extension_data_source ec_source_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple = NULL;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_extension_data_source];
        bool nulls[Natts_pg_extension_data_source] = {false};
        bool replaces[Natts_pg_extension_data_source] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        errno_t ret;

        tuple = SearchSysCache1(DATASOURCEOID, ObjectIdGetDatum(srcid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for data source %u", srcid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        ec_source_tuple = (Form_pg_extension_data_source)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = ec_source_tuple->srcowner;
        aclDatum = SysCacheGetAttr(DATASOURCEOID, tuple, Anum_pg_extension_data_source_srcacl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_DATA_SOURCE, ownerId);

            /* There are no old member roles according to the catalogs */
            oldmembers = NULL;
            noldmembers = 0;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);

            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            srcid,
            grantorId,
            ACL_KIND_DATA_SOURCE,
            NameStr(ec_source_tuple->srcname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        ret = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ret, "\0", "\0");
        ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(ret, "\0", "\0");
        ret = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(ret, "\0", "\0");

        replaces[Anum_pg_extension_data_source_srcacl - 1] = true;
        values[Anum_pg_extension_data_source_srcacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            DataSourceRelationId, HeapTupleGetOid(tuple), 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static void ExecGrant_Cmk(InternalGrant *istmt)
{
    Relation relation;
    ListCell *cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_KEY;
        istmt->ddl_privileges = ACL_ALL_DDL_RIGHTS_KEY;
    }

    relation = heap_open(ClientLogicGlobalSettingsId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid keyId = lfirst_oid(cell);
        Form_gs_client_global_keys cmk_tuple;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl *old_acl = NULL;
        Acl *new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple;
        HeapTuple newtuple;
        Datum values[Natts_gs_client_global_keys];
        bool nulls[Natts_gs_client_global_keys];
        bool replaces[Natts_gs_client_global_keys];
        int noldmembers;
        int nnewmembers;
        Oid *oldmembers = NULL;
        Oid *newmembers = NULL;
        errno_t ret = EOK;

        tuple = SearchSysCache1(GLOBALSETTINGOID, ObjectIdGetDatum(keyId));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for client master key %u", keyId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        }

        cmk_tuple = (Form_gs_client_global_keys)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = cmk_tuple->key_owner;
        aclDatum = SysCacheGetAttr(GLOBALSETTINGOID, tuple, Anum_gs_client_global_keys_key_acl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_GLOBAL_SETTING, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId, &grantorId,
            &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant, avail_goptions, avail_ddl_goptions, istmt->all_privs,
            istmt->privileges, istmt->ddl_privileges, keyId, grantorId, ACL_KIND_GLOBAL_SETTING,
            NameStr(cmk_tuple->global_key_name), 0, NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl, istmt->is_grant, istmt->grant_option, istmt->behavior, istmt->grantees,
            this_privileges, grantorId, ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        ret = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ret, "\0", "\0");
        ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(ret, "\0", "\0");
        ret = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(ret, "\0", "\0");

        replaces[Anum_gs_client_global_keys_key_acl - 1] = true;
        values[Anum_gs_client_global_keys_key_acl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(ClientLogicGlobalSettingsId, keyId, 0, ownerId, noldmembers, oldmembers, nnewmembers,
            newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}


static void ExecGrant_Cek(InternalGrant *istmt)
{
    Relation relation;
    ListCell *cell = NULL;

    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_KEY;
        istmt->ddl_privileges = ACL_ALL_DDL_RIGHTS_KEY;
    }

    relation = heap_open(ClientLogicColumnSettingsId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid keyId = lfirst_oid(cell);
        Form_gs_column_keys cek_tuple;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl *old_acl = NULL;
        Acl *new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple tuple;
        HeapTuple newtuple;
        Datum values[Natts_gs_column_keys];
        bool nulls[Natts_gs_column_keys];
        bool replaces[Natts_gs_column_keys];
        int noldmembers;
        int nnewmembers;
        Oid *oldmembers = NULL;
        Oid *newmembers = NULL;
        errno_t ret = EOK;

        tuple = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(keyId));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for column encryption key %u", keyId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        }

        cek_tuple = (Form_gs_column_keys)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = cek_tuple->key_owner;
        aclDatum = SysCacheGetAttr(COLUMNSETTINGOID, tuple, Anum_gs_column_keys_key_acl, &isNull);
        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_COLUMN_SETTING, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId, &grantorId,
            &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant, avail_goptions, avail_ddl_goptions, istmt->all_privs,
            istmt->privileges, istmt->ddl_privileges, keyId, grantorId, ACL_KIND_COLUMN_SETTING,
            NameStr(cek_tuple->column_key_name), 0, NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl, istmt->is_grant, istmt->grant_option, istmt->behavior, istmt->grantees,
            this_privileges, grantorId, ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        ret = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ret, "\0", "\0");
        ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(ret, "\0", "\0");
        ret = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(ret, "\0", "\0");

        replaces[Anum_gs_column_keys_key_acl - 1] = true;
        values[Anum_gs_column_keys_key_acl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(ClientLogicColumnSettingsId, keyId, 0, ownerId, noldmembers, oldmembers, nnewmembers,
            newmembers);

        ReleaseSysCache(tuple);

        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

/*
 * Brief		: Grant privileges to operate directory.
 * Description	: Grant privileges to user to operate directory, store the
 * 				  privileges information to pg_directory's dircacl item.
 */
static void ExecGrant_Directory(InternalGrant* istmt)
{
    Relation relation = NULL;
    ListCell* cell = NULL;
    errno_t rc;
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS && istmt->ddl_privileges == ACL_NO_DDL_RIGHTS) {
        istmt->privileges = ACL_ALL_RIGHTS_DIRECTORY;
        istmt->ddl_privileges = (t_thrd.proc->workingVersionNum >= PRIVS_DIRECTORY_VERSION_NUM) ?
            ACL_ALL_DDL_RIGHTS_DIRECTORY : ACL_NO_DDL_RIGHTS;
    }

    relation = heap_open(PgDirectoryRelationId, RowExclusiveLock);

    foreach (cell, istmt->objects) {
        Oid dirId = lfirst_oid(cell);
        Form_pg_directory pg_directory_tuple = NULL;
        Datum aclDatum;
        bool isNull = false;
        AclMode avail_goptions;
        AclMode avail_ddl_goptions;
        AclMode this_privileges[PRIVS_ATTR_NUM];
        Acl* old_acl = NULL;
        Acl* new_acl = NULL;
        Oid grantorId;
        Oid ownerId;
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_directory];
        bool nulls[Natts_pg_directory] = {false};
        bool replaces[Natts_pg_directory] = {false};
        int noldmembers;
        int nnewmembers;
        Oid* oldmembers = NULL;
        Oid* newmembers = NULL;
        HeapTuple tuple = NULL;

        /* Search syscache for pg_directory */
        tuple = SearchSysCache1(DIRECTORYOID, ObjectIdGetDatum(dirId));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for directory %u", dirId), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
            return;
        }

        pg_directory_tuple = (Form_pg_directory)GETSTRUCT(tuple);

        /*
         * Get owner ID and working copy of existing ACL. If there's no ACL,
         * substitute the proper default.
         */
        ownerId = pg_directory_tuple->owner;
        aclDatum = heap_getattr(tuple, Anum_pg_directory_directory_acl, RelationGetDescr(relation), &isNull);

        if (isNull) {
            old_acl = acldefault(ACL_OBJECT_DIRECTORY, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = NULL;
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(GetUserId(), istmt->privileges, istmt->ddl_privileges, old_acl, ownerId,
            &grantorId, &avail_goptions, &avail_ddl_goptions);

        /*
         * Restrict the privileges to what we can actually grant, and emit the
         * standards-mandated warning and error messages.
         */
        restrict_and_check_grant(this_privileges, istmt->is_grant,
            avail_goptions,
            avail_ddl_goptions,
            istmt->all_privs,
            istmt->privileges,
            istmt->ddl_privileges,
            dirId,
            grantorId,
            ACL_KIND_DIRECTORY,
            NameStr(pg_directory_tuple->dirname),
            0,
            NULL);

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(old_acl,
            istmt->is_grant,
            istmt->grant_option,
            istmt->behavior,
            istmt->grantees,
            this_privileges,
            grantorId,
            ownerId);

        /*
         * We need the members of both old and new ACLs so we can correct the
         * shared dependency information.
         */
        nnewmembers = aclmembers(new_acl, &newmembers);

        /* finished building new ACL value, now insert it */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_directory_directory_acl - 1] = true;
        values[Anum_pg_directory_directory_acl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        /* keep the catalog indexes up to date */
        CatalogUpdateIndexes(relation, newtuple);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            PgDirectoryRelationId, dirId, 0, ownerId, noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree_ext(new_acl);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
    }

    heap_close(relation, RowExclusiveLock);
}

static AclMode string_to_privilege(const char* privname)
{
    unsigned int i;
    priv_map dml_priv_map[] = {
        {"select", ACL_SELECT}, {"insert", ACL_INSERT}, {"update", ACL_UPDATE},
        {"delete", ACL_DELETE}, {"truncate", ACL_TRUNCATE}, {"references", ACL_REFERENCES}, {"trigger", ACL_TRIGGER},
        {"execute", ACL_EXECUTE}, {"usage", ACL_USAGE}, {"create", ACL_CREATE}, {"temp", ACL_CREATE_TEMP},
        {"temporary", ACL_CREATE_TEMP}, {"connect", ACL_CONNECT}, {"compute", ACL_COMPUTE}, {"read", ACL_READ},
        {"write", ACL_WRITE}, {"rule", 0} /* ignore old RULE privileges */
    };

    for (i = 0; i < lengthof(dml_priv_map); i++) {
        if (strcmp(privname, dml_priv_map[i].name) == 0) {
            return dml_priv_map[i].value;
        }
    }

    if (t_thrd.proc->workingVersionNum >= PRIVS_VERSION_NUM) {
        priv_map ddl_priv_map [] = {
            {"index", ACL_INDEX}, { "vacuum", ACL_VACUUM}, {"alter", ACL_ALTER},
            {"drop", ACL_DROP}, {"comment", ACL_COMMENT}
        };

        for (i = 0; i < lengthof(ddl_priv_map); i++) {
            if (strcmp(privname, ddl_priv_map[i].name) == 0) {
                return ddl_priv_map[i].value;
            }
        }
    }

    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYNTAX_ERROR),
        errmsg("unrecognized privilege type \"%s\"", privname), errdetail("N/A"),
            errcause("The privilege type is not supported."),
                erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types.")));
    return 0; /* appease compiler */
}

static const char* privilege_to_string(AclMode privilege)
{
    priv_map priv_map[] = {
        {"SELECT", ACL_SELECT}, {"INSERT", ACL_INSERT}, {"UPDATE", ACL_UPDATE},
        {"DELETE", ACL_DELETE}, {"TRUNCATE", ACL_TRUNCATE}, {"REFERENCES", ACL_REFERENCES}, {"TRIGGER", ACL_TRIGGER},
        {"EXECUTE", ACL_EXECUTE}, {"USAGE", ACL_USAGE}, {"CREATE", ACL_CREATE}, {"TEMP", ACL_CREATE_TEMP},
        {"CONNECT", ACL_CONNECT}, {"COMPUTE", ACL_COMPUTE}, {"READ", ACL_READ}, {"WRITE", ACL_WRITE},
        {"ALTER", ACL_ALTER}, {"DROP", ACL_DROP}, {"COMMENT", ACL_COMMENT}, {"INDEX", ACL_INDEX}, {"VACUUM", ACL_VACUUM}
    };

    for (unsigned int i = 0; i < lengthof(priv_map); i++) {
        if (privilege == priv_map[i].value) {
            return priv_map[i].name;
        }
    }

    ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
        errmsg("unrecognized privilege: %d", (int)privilege), errdetail("N/A"),
            errcause("The privilege type is not supported."),
                erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types.")));
    return NULL; /* appease compiler */
}

/*
 * Standardized reporting of aclcheck permissions failures.
 *
 * Note: we do not double-quote the %s's below, because many callers
 * supply strings that might be already quoted.
 */
static const char* const no_priv_msg[MAX_ACL_KIND] = {
    /* ACL_KIND_COLUMN */
    gettext_noop("permission denied for column %s"),
    /* ACL_KIND_CLASS */
    gettext_noop("permission denied for relation %s"),
    /* ACL_KIND_SEQUENCE */
    gettext_noop("permission denied for sequence %s"),
    /* ACL_KIND_DATABASE */
    gettext_noop("permission denied for database %s"),
    /* ACL_KIND_PROC */
    gettext_noop("permission denied for function %s"),
    /* ACL_KIND_OPER */
    gettext_noop("permission denied for operator %s"),
    /* ACL_KIND_TYPE */
    gettext_noop("permission denied for type %s"),
    /* ACL_KIND_LANGUAGE */
    gettext_noop("permission denied for language %s"),
    /* ACL_KIND_LARGEOBJECT */
    gettext_noop("permission denied for large object %s"),
    /* ACL_KIND_NAMESPACE */
    gettext_noop("permission denied for schema %s"),
    /* ACL_KIND_NODEGROUP */
    gettext_noop("permission denied for node group %s"),
    /* ACL_KIND_OPCLASS */
    gettext_noop("permission denied for operator class %s"),
    /* ACL_KIND_OPFAMILY */
    gettext_noop("permission denied for operator family %s"),
    /* ACL_KIND_COLLATION */
    gettext_noop("permission denied for collation %s"),
    /* ACL_KIND_CONVERSION */
    gettext_noop("permission denied for conversion %s"),
    /* ACL_KIND_TABLESPACE */
    gettext_noop("permission denied for tablespace %s"),
    /* ACL_KIND_TSDICTIONARY */
    gettext_noop("permission denied for text search dictionary %s"),
    /* ACL_KIND_TSCONFIGURATION */
    gettext_noop("permission denied for text search configuration %s"),
    /* ACL_KIND_FDW */
    gettext_noop("permission denied for foreign-data wrapper %s"),
    /* ACL_KIND_FOREIGN_SERVER */
    gettext_noop("permission denied for foreign server %s"),
    /* ACL_KIND_EXTENSION */
    gettext_noop("permission denied for extension %s"),
    /* ACL_KIND_DATA_SOURCE */
    gettext_noop("permission denied for data source %s"),
    /* ACL_KIND_DIRECTORY */
    gettext_noop("permission denied for directory %s"),
    /* ACL_KIND_COLUMN_SETTING */
    gettext_noop("permission denied for column encryption key %s"),
    /* ACL_KIND_GLOBAL_SETTING */
    gettext_noop("permission denied for client master key %s"),
    /* ACL_KIND_PACKAGE */
    gettext_noop("permission denied for package %s"),
    /* ACL_KIND_EVENT_TRIGGER */
    gettext_noop("permission denied for event trigger %s"),
};

static const char* const not_owner_msg[MAX_ACL_KIND] = {
    /* ACL_KIND_COLUMN */
    gettext_noop("must be owner of relation %s"),
    /* ACL_KIND_CLASS */
    gettext_noop("must be owner of relation %s"),
    /* ACL_KIND_SEQUENCE */
    gettext_noop("must be owner of sequence %s"),
    /* ACL_KIND_DATABASE */
    gettext_noop("must be owner of database %s"),
    /* ACL_KIND_PROC */
    gettext_noop("must be owner of function %s"),
    /* ACL_KIND_OPER */
    gettext_noop("must be owner of operator %s"),
    /* ACL_KIND_TYPE */
    gettext_noop("must be owner of type %s"),
    /* ACL_KIND_LANGUAGE */
    gettext_noop("must be owner of language %s"),
    /* ACL_KIND_LARGEOBJECT */
    gettext_noop("must be owner of large object %s"),
    /* ACL_KIND_NAMESPACE */
    gettext_noop("must be owner of schema %s"),
    /* ACL_KIND_NODEGROUP */
    gettext_noop("must be owner of node group %s"),
    /* ACL_KIND_OPCLASS */
    gettext_noop("must be owner of operator class %s"),
    /* ACL_KIND_OPFAMILY */
    gettext_noop("must be owner of operator family %s"),
    /* ACL_KIND_COLLATION */
    gettext_noop("must be owner of collation %s"),
    /* ACL_KIND_CONVERSION */
    gettext_noop("must be owner of conversion %s"),
    /* ACL_KIND_TABLESPACE */
    gettext_noop("must be owner of tablespace %s"),
    /* ACL_KIND_TSDICTIONARY */
    gettext_noop("must be owner of text search dictionary %s"),
    /* ACL_KIND_TSCONFIGURATION */
    gettext_noop("must be owner of text search configuration %s"),
    /* ACL_KIND_FDW */
    gettext_noop("must be owner of foreign-data wrapper %s"),
    /* ACL_KIND_FOREIGN_SERVER */
    gettext_noop("must be owner of foreign server %s"),
    /* ACL_KIND_EXTENSION */
    gettext_noop("must be owner of extension %s"),
    /* ACL_KIND_DATA_SOURCE */
    gettext_noop("must be owner of data source %s"),
    /* ACL_KIND_DIRECTORY */
    gettext_noop("must be owner of directory %s"),
    /* ACL_KIND_COLUMN_SETTING */
    gettext_noop("must be owner of column encryption key %s"),
    /* ACL_KIND_GLOBAL_SETTING */
    gettext_noop("must be owner of client master key %s"),
    /* ACL_KIND_PACKAGE */
    gettext_noop("must be owner of package %s"),
    /* ACL_KIND_PUBLICATION */
    gettext_noop("must be owner of publication %s"),
    /* ACL_KIND_SUBSCRIPTION */
    gettext_noop("must be owner of subscription %s"),
    /* ACL_KIND_EVENT_TRIGGER */
    gettext_noop("must be owner of event trigger %s"),
};

void aclcheck_error(AclResult aclerr, AclObjectKind objectkind, const char* objectname)
{
    switch (aclerr) {
        case ACLCHECK_OK:
            /* no error, so return to caller */
            break;
        case ACLCHECK_NO_PRIV:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg(no_priv_msg[objectkind], objectname), errdetail("N/A"),
                    errcause("Insufficient privileges for the object."),
                        erraction("Select the system tables to get the acl of the object.")));
            break;
        case ACLCHECK_NOT_OWNER:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg(not_owner_msg[objectkind], objectname), errdetail("N/A"),
                    errcause("Not the owner of the object."),
                        erraction("Select the system tables to get the owner of the object.")));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized AclResult"), errdetail("unrecognized AclResult: %d", (int)aclerr),
                    errcause("System error."), erraction("Contact engineer to support.")));
            break;
    }
}

void aclcheck_error_col(AclResult aclerr, AclObjectKind objectkind, const char* objectname, const char* colname)
{
    switch (aclerr) {
        case ACLCHECK_OK:
            /* no error, so return to caller */
            break;
        case ACLCHECK_NO_PRIV:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for column \"%s\" of relation \"%s\"", colname, objectname), errdetail("N/A"),
                    errcause("Insufficient privileges for the column."),
                        erraction("Select the system tables to get the acl of the column.")));
            break;
        case ACLCHECK_NOT_OWNER:
            /* relation msg is OK since columns don't have separate owners */
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg(not_owner_msg[objectkind], objectname), errdetail("N/A"),
                    errcause("Not the owner of the column."),
                        erraction("Select the system tables to get the owner of the column.")));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized AclResult"), errdetail("unrecognized AclResult: %d", (int)aclerr),
                    errcause("System error."), erraction("Contact engineer to support.")));
            break;
    }
}

/*
 * Special common handling for types: use element type instead of array type,
 * and format nicely
 */
void aclcheck_error_type(AclResult aclerr, Oid typeOid)
{
    Oid element_type = get_element_type(typeOid);

    aclcheck_error(aclerr, ACL_KIND_TYPE, format_type_be(element_type ? element_type : typeOid));
}

/* Check if given user has rolcatupdate privilege according to pg_authid */
static bool has_rolcatupdate(Oid roleid)
{
    bool rolcatupdate = false;
    HeapTuple tuple = NULL;

    tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("role with OID %u does not exist", roleid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    rolcatupdate = ((Form_pg_authid)GETSTRUCT(tuple))->rolcatupdate;

    ReleaseSysCache(tuple);

    return rolcatupdate;
}

/*
 * Relay for the various pg_*_mask routines depending on object kind
 */
static AclMode pg_aclmask(
    AclObjectKind objkind, Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how)
{
    switch (objkind) {
        case ACL_KIND_COLUMN:
            return pg_class_aclmask(table_oid, roleid, mask, how) |
                   pg_attribute_aclmask(table_oid, attnum, roleid, mask, how);
        case ACL_KIND_CLASS:
        case ACL_KIND_SEQUENCE:
            return pg_class_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_DATABASE:
            return pg_database_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_PROC:
            return pg_proc_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_PACKAGE:
            return pg_package_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_LANGUAGE:
            return pg_language_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_LARGEOBJECT:
            return pg_largeobject_aclmask_snapshot(table_oid, roleid, mask, how, SnapshotNow);
        case ACL_KIND_NAMESPACE:
            return pg_namespace_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_NODEGROUP:
            return pg_nodegroup_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_TABLESPACE:
            return pg_tablespace_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_FDW:
            return pg_foreign_data_wrapper_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_FOREIGN_SERVER:
            return pg_foreign_server_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_EVENT_TRIGGER:
            elog(ERROR, "grantable rights not supported for event triggers");
            /* not reached, but keep compiler quiet */
            return ACL_NO_RIGHTS;
        case ACL_KIND_TYPE:
            return pg_type_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_DATA_SOURCE:
            return pg_extension_data_source_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_DIRECTORY:
            return pg_directory_aclmask(table_oid, roleid, mask, how);
        case ACL_KIND_COLUMN_SETTING:
            return gs_sec_cek_aclcheck(table_oid, roleid, mask, how);
        case ACL_KIND_GLOBAL_SETTING:
            return gs_sec_cmk_aclcheck(table_oid, roleid, mask, how);
        default:
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized objkind: %d", (int)objkind), errdetail("N/A"),
                    errcause("The object type is not supported for privilege check."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported object types.")));
            /* not reached, but keep compiler quiet */
            return ACL_NO_RIGHTS;
    }
}

/* ****************************************************************
 * Exported routines for examining a user's privileges for various objects
 *
 * See aclmask() for a description of the common API for these functions.
 *
 * Note: we give lookup failure the full ereport treatment because the
 * has_xxx_privilege() family of functions allow users to pass any random
 * OID to these functions.
 * ****************************************************************
 */
/*
 * Exported routine for examining a user's privileges for a column
 *
 * Note: this considers only privileges granted specifically on the column.
 * It is caller's responsibility to take relation-level privileges into account
 * as appropriate.	(For the same reason, we have no special case for
 * superuser-ness here.)
 */
AclMode pg_attribute_aclmask(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple classTuple = NULL;
    HeapTuple attTuple = NULL;
    Form_pg_class classForm = NULL;
    Form_pg_attribute attributeForm = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /*
     * First, get the column's ACL from its pg_attribute entry
     */
    attTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(table_oid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(attTuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("attribute %d of relation with OID %u does not exist", attnum, table_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    attributeForm = (Form_pg_attribute)GETSTRUCT(attTuple);

    /* Throw error on dropped columns, too */
    if (attributeForm->attisdropped)
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("the column has been dropped"),
            errdetail("attribute %d of relation with OID %u does not exist", attnum, table_oid),
                errcause("The column does not exist."), erraction("Check whether the column exists.")));

    aclDatum = SysCacheGetAttr(ATTNUM, attTuple, Anum_pg_attribute_attacl, &isNull);

    /*
     * Here we hard-wire knowledge that the default ACL for a column grants no
     * privileges, so that we can fall out quickly in the very common case
     * where attacl is null.
     */
    if (isNull) {
        ReleaseSysCache(attTuple);
        return 0;
    }

    /*
     * Must get the relation's ownerId from pg_class.  Since we already found
     * a pg_attribute entry, the only likely reason for this to fail is that a
     * concurrent DROP of the relation committed since then (which could only
     * happen if we don't have lock on the relation).  We prefer to report "no
     * privileges" rather than failing in such a case, so as to avoid unwanted
     * failures in has_column_privilege() tests.
     */
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(attTuple);
        return 0;
    }
    classForm = (Form_pg_class)GETSTRUCT(classTuple);

    ownerId = classForm->relowner;
    Oid namespaceId = classForm->relnamespace;

    ReleaseSysCache(classTuple);

    /* detoast column's ACL if necessary */
    acl = DatumGetAclP(aclDatum);

    if (IsMonitorSpace(namespaceId)) {
       result = aclmask_without_sysadmin(acl, roleid, ownerId, mask, how);
    } else {
        result = aclmask(acl, roleid, ownerId, mask, how);
    }

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(attTuple);

    return result;
}

static AclMode check_dml_privilege(Form_pg_class classForm, AclMode mask, Oid roleid, AclMode result)
{
    if (is_role_independent(classForm->relowner)) {
        return result;
    }
    switch (classForm->relkind) {
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        case RELKIND_COMPOSITE_TYPE:
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            if (HasSpecAnyPriv(roleid, SELECT_ANY_SEQUENCE, false)) {
                result |= ACL_USAGE | ACL_SELECT | ACL_UPDATE;
            }
            break;
        /* table */
        default:
            if ((mask & ACL_SELECT) && !(result & ACL_SELECT)) {
                if (HasSpecAnyPriv(roleid, SELECT_ANY_TABLE, false)) {
                    result |= ACL_SELECT;
                }
            }

            if ((mask & ACL_INSERT) && !(result & ACL_INSERT)) {
                if (HasSpecAnyPriv(roleid, INSERT_ANY_TABLE, false)) {
                    result |= ACL_INSERT;
                }
            }
            if ((mask & ACL_UPDATE) && !(result & ACL_UPDATE)) {
                if (HasSpecAnyPriv(roleid, UPDATE_ANY_TABLE, false)) {
                    result |= ACL_UPDATE;
                }
            }
            if ((mask & ACL_DELETE) && !(result & ACL_DELETE)) {
                if (HasSpecAnyPriv(roleid, DELETE_ANY_TABLE, false)) {
                    result |= ACL_DELETE;
                }
            }
            break;
    }
    return result;
}

static AclMode check_ddl_privilege(char relkind, AclMode mask, Oid roleid, AclMode result)
{
    mask = REMOVE_DDL_FLAG(mask);
    switch (relkind) {
        case RELKIND_COMPOSITE_TYPE:
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            break;
        case RELKIND_LARGE_SEQUENCE:
        case RELKIND_SEQUENCE:
            if ((mask & ACL_ALTER) && !(result & ACL_ALTER)) {
                if (HasSpecAnyPriv(roleid, ALTER_ANY_SEQUENCE, false)) {
                    result |= ACL_ALTER;
                }
            }
            if ((mask & ACL_DROP) && !(result & ACL_DROP)) {
                if (HasSpecAnyPriv(roleid, DROP_ANY_SEQUENCE, false)) {
                    result |= ACL_DROP;
                }
            }
            break;
        /* table */
        default:
            if ((mask & ACL_DROP) && !(result & ACL_DROP)) {
                if (HasSpecAnyPriv(roleid, DROP_ANY_TABLE, false)) {
                    result |= ACL_DROP;
                }
            }
            if ((mask & ACL_ALTER) && !(result & ACL_ALTER)) {
                if (HasSpecAnyPriv(roleid, ALTER_ANY_TABLE, false)) {
                    result |= ACL_ALTER;
                }
            }
            break;
    }
    return result;
}
/*
 * Exported routine for examining a user's privileges for a table
 */
AclMode pg_class_aclmask(Oid table_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Form_pg_class classForm;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    /* remove ddl privileges flag from Aclitem */
    mask = REMOVE_DDL_FLAG(mask);

    /*
     * Must get the relation's tuple from pg_class
     */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("relation with OID %u does not exist", table_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    classForm = (Form_pg_class)GETSTRUCT(tuple);

    /* Check current user has privilige to this group */
    if (IS_PGXC_COORDINATOR && !IsInitdb && check_nodegroup && is_pgxc_class_table(table_oid) &&
        roleid != classForm->relowner) {
        Oid group_oid = get_pgxc_class_groupoid(table_oid);
        AclResult aclresult = ACLCHECK_OK;
        if (InvalidOid == group_oid) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid group"),
                errdetail("computing nodegroup is not a valid group."),
                    errcause("System error."), erraction("Contact engineer to support.")));
        } else {
            aclresult = pg_nodegroup_aclcheck(group_oid, roleid, ACL_USAGE);
            if (aclresult != ACLCHECK_OK) {
                aclcheck_error(aclresult, ACL_KIND_NODEGROUP, get_pgxc_groupname(group_oid));
            }
        }
    }

    /*
     * Deny anyone permission to update a system catalog unless
     * pg_authid.rolcatupdate is set.	(This is to let superusers protect
     * themselves from themselves.)  Also allow it if g_instance.attr.attr_common.allowSystemTableMods.
     *
     * As of 7.4 we have some updatable system views; those shouldn't be
     * protected in this way.  Assume the view rules can take care of
     * themselves.	ACL_USAGE is if we ever have system sequences.
     */
    if (!is_ddl_privileges && (mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE)) 
        && IsSystemClass(classForm) &&
        classForm->relkind != RELKIND_VIEW && classForm->relkind != RELKIND_CONTQUERY && !has_rolcatupdate(roleid) &&
        !g_instance.attr.attr_common.allowSystemTableMods) {
#ifdef ACLDEBUG
        elog(DEBUG2, "permission denied for system catalog update");
#endif
        mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE);
    }

    /* For relations in schema dbe_perf and schema snapshot,
     * initial user and monitorsdmin bypass all permission-checking.
     */
    Oid namespaceId = classForm->relnamespace;
    if (IsMonitorSpace(namespaceId) && (roleid == INITIAL_USER_ID || isMonitoradmin(roleid))) {
        ReleaseSysCache(tuple);
        return mask;
    }

    /* Blockchain hist table cannot be modified */
    if (table_oid == GsGlobalChainRelationId || classForm->relnamespace == PG_BLOCKCHAIN_NAMESPACE) {
        if (isRelSuperuser() || isAuditadmin(roleid)) {
            mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE | ACL_REFERENCES);
        } else {
            mask &= ACL_NO_RIGHTS;
        }
        ReleaseSysCache(tuple);
        return mask;
    }

    /* Otherwise, superusers bypass all permission-checking, except access independent role's objects. */
    /* Database Security:  Support separation of privilege. */
    if (!is_ddl_privileges && !IsMonitorSpace(namespaceId) && (superuser_arg(roleid) || systemDBA_arg(roleid)) &&
        ((classForm->relowner == roleid) || !is_role_independent(classForm->relowner) ||
            independent_priv_aclcheck(mask, classForm->relkind))) {
#ifdef ACLDEBUG
        elog(DEBUG2, "OID %u is system admin, home free", roleid);
#endif
        ReleaseSysCache(tuple);
        return mask;
    }

    if (is_security_policy_relation(table_oid) && isPolicyadmin(roleid)) {
        ReleaseSysCache(tuple);
        return mask;
    }
    /* When meet pg_catalog.gs_global_config and the user has CREATEROLE privilege,
       bypass all permission-checking */
    if (table_oid == GsGlobalConfigRelationId && has_createrole_privilege(roleid)) {
        ReleaseSysCache(tuple);
        return mask;
    }

    /*
    * When operation_mode = on, allow opradmin to update pgxc_node and select all system classes;
    * And also allow opradmin to access to users'tables
    */
    if (!is_ddl_privileges && isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode) {
        if (table_oid == PgxcNodeRelationId) {
            mask |= ACL_UPDATE;
            ReleaseSysCache(tuple);
            return mask;
        } else if (IsSystemClass(classForm)) {
            ReleaseSysCache(tuple);
            return mask;
        } else if ((mask & (ACL_SELECT | ACL_INSERT | ACL_TRUNCATE | ACL_USAGE)) &&
            !is_role_independent(classForm->relowner)) {
            mask &= ~(ACL_UPDATE | ACL_DELETE);
            ReleaseSysCache(tuple);
            return mask;
        }
    }

    /*
     * Normal case: get the relation's ACL from pg_class
     */
    ownerId = classForm->relowner;

    aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        switch (classForm->relkind) {
            case RELKIND_SEQUENCE:
            case RELKIND_LARGE_SEQUENCE:
                acl = acldefault(ACL_OBJECT_SEQUENCE, ownerId);
                break;
            default:
                acl = acldefault(ACL_OBJECT_RELATION, ownerId);
                break;
        }
        aclDatum = (Datum)0;
    } else {
        /* detoast rel's ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    if (check_nodegroup) {
        // support view and sequeuce
        check_nodegroup_privilege(roleid, ownerId, ACL_USAGE);
    }

    if (is_ddl_privileges) {
        mask = ADD_DDL_FLAG(mask);
    }
    if (IsMonitorSpace(namespaceId)) {
        result = aclmask_without_sysadmin(acl, roleid, ownerId, mask, how);
    } else {
        result = aclmask(acl, roleid, ownerId, mask, how);
    }

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    if ((how == ACLMASK_ANY && result != 0) || IsSysSchema(namespaceId)) {
        ReleaseSysCache(tuple);
        return result;
    }
    if (is_ddl_privileges) {
        result = check_ddl_privilege(classForm->relkind, mask, roleid, result);
    } else {
        result = check_dml_privilege(classForm, mask, roleid, result);
    }
    ReleaseSysCache(tuple);
    return result;
}

/*
 * Exported routine for examining a user's privileges for a database
 */
AclMode pg_database_aclmask(Oid db_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    if (isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode) {
        return REMOVE_DDL_FLAG(mask);
    }

    /*
     * Get the database's ACL from pg_database
     */
    tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(db_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("database with OID %u does not exist", db_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_database)GETSTRUCT(tuple))->datdba;

    aclDatum = SysCacheGetAttr(DATABASEOID, tuple, Anum_pg_database_datacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a pg_directory
 */
AclMode pg_directory_aclmask(Oid dir_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /*
     * when enable_access_server_directory is off, only initial user bypass all permission checking
     * otherwise, superuser can bypass all permission checking
     */
    if ((!u_sess->attr.attr_storage.enable_access_server_directory && superuser_arg_no_seperation(roleid)) ||
        (u_sess->attr.attr_storage.enable_access_server_directory &&
            (superuser_arg(roleid) || systemDBA_arg(roleid))))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Get the database's ACL from pg_directory
     */
    tuple = SearchSysCache1(DIRECTORYOID, ObjectIdGetDatum(dir_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("directory with OID %u does not exist", dir_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_directory)GETSTRUCT(tuple))->owner;

    aclDatum = SysCacheGetAttr(DIRECTORYOID, tuple, Anum_pg_directory_directory_acl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_DIRECTORY, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a function
 */
AclMode pg_proc_aclmask(Oid proc_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup)
{
    AclMode result;
    HeapTuple tuple = NULL;
    HeapTuple pkgTuple = NULL;
    Datum aclDatum;
    Datum packageOidDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId = InvalidOid;
    Oid packageOid = InvalidOid;

    if(IsSystemObjOid(proc_oid) && isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode){
        return REMOVE_DDL_FLAG(mask);
    }

    /*
     * Get the function's ACL from pg_proc
     */
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function with OID %u does not exist", proc_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_proc)GETSTRUCT(tuple))->proowner;
    packageOidDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_packageid, &isNull);
    if (!isNull) {
        packageOid = DatumGetObjectId(packageOidDatum);
    }
    /* For procs in schema dbe_perf and schema snapshot,
     * initial user and monitorsdmin bypass all permission-checking.
     */
    Oid namespaceId = ((Form_pg_proc) GETSTRUCT(tuple))->pronamespace;
    if (IsMonitorSpace(namespaceId) && (roleid == INITIAL_USER_ID || isMonitoradmin(roleid))) {
        ReleaseSysCache(tuple);
        return REMOVE_DDL_FLAG(mask);
    }
    /* Superusers bypass all permission checking for all procs not in schema dbe_perf and schema snapshot. */
    /* Database Security:  Support separation of privilege.*/
    if (!IsMonitorSpace(namespaceId) && (superuser_arg(roleid) || systemDBA_arg(roleid))) {
        ReleaseSysCache(tuple);
        return REMOVE_DDL_FLAG(mask);
    }
    if (!OidIsValid(packageOid)) {
        aclDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proacl, &isNull);
    } else {
        pkgTuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
        if (!HeapTupleIsValid(pkgTuple)) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("package with OID %u does not exist", packageOid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));
        }
        aclDatum = SysCacheGetAttr(PACKAGEOID, pkgTuple, Anum_gs_package_pkgacl, &isNull);
    }
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    if (check_nodegroup) {
        check_nodegroup_privilege(roleid, ownerId, ACL_USAGE);
    }

    if (IsMonitorSpace(namespaceId)) {
        result = aclmask_without_sysadmin(acl, roleid, ownerId, mask, how);
    } else {
        result = aclmask(acl, roleid, ownerId, mask, how);
    }
    if ((mask & ACL_EXECUTE) && !(result & ACL_EXECUTE) && !IsSysSchema(namespaceId) &&
        HasSpecAnyPriv(roleid, EXECUTE_ANY_FUNCTION, false)) {
        result |= ACL_EXECUTE;
    }
    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);
    if (HeapTupleIsValid(pkgTuple)) {
        ReleaseSysCache(pkgTuple);
    }
    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a package
 */
AclMode pg_package_aclmask(Oid packageOid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    HeapTuple pkgTuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId = InvalidOid;

    /*
     * Get the package's ACL from gs_package
     */
    tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_PACKAGE),
            errmsg("package with OID %u does not exist", packageOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    ownerId = ((Form_gs_package)GETSTRUCT(tuple))->pkgowner;

    pkgTuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(packageOid));
    if (!HeapTupleIsValid(pkgTuple)) {
        ReleaseSysCache(pkgTuple);
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("package with OID %u does not exist", packageOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    aclDatum = SysCacheGetAttr(PACKAGEOID, pkgTuple, Anum_gs_package_pkgacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_PACKAGE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }
    result = aclmask(acl, roleid, ownerId, mask, how);
    Oid namespaceId = ((Form_gs_package) GETSTRUCT(tuple))->pkgnamespace;
    if ((mask & ACL_EXECUTE) && !(result & ACL_EXECUTE) && !IsSysSchema(namespaceId) &&
        HasSpecAnyPriv(roleid, EXECUTE_ANY_PACKAGE, false)) {
        result |= ACL_EXECUTE;
    }
    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);
    if (HeapTupleIsValid(pkgTuple)) {
        ReleaseSysCache(pkgTuple);
    }
    ReleaseSysCache(tuple);

    return result;
}


/*
 * Exported routine for examining a user's privileges for a client master key
 */
AclMode gs_sec_cmk_aclmask(Oid global_setting_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup)
{
    AclMode result;
    HeapTuple tuple;
    Datum aclDatum;
    bool isNull = false;
    Acl *acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Get the function's ACL from gs_sec_client_master_key
     */
    tuple = SearchSysCache1(GLOBALSETTINGOID, ObjectIdGetDatum(global_setting_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_KEY),
            errmsg("client master key with OID %u does not exist", global_setting_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }

    ownerId = ((Form_gs_client_global_keys)GETSTRUCT(tuple))->key_owner;

    aclDatum = SysCacheGetAttr(GLOBALSETTINGOID, tuple, Anum_gs_client_global_keys_key_acl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_GLOBAL_SETTING, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    if (check_nodegroup) {
        check_nodegroup_privilege(roleid, ownerId, ACL_USAGE);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a language
 */
AclMode pg_language_aclmask(Oid lang_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl *acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Get the language's ACL from pg_language
     */
    tuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(lang_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("language with OID %u does not exist", lang_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_language)GETSTRUCT(tuple))->lanowner;

    aclDatum = SysCacheGetAttr(LANGOID, tuple, Anum_pg_language_lanacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId, lang_oid);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a function
 */
AclMode gs_sec_cek_aclmask(Oid column_setting_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup)
{
    AclMode     result;
    HeapTuple   tuple;
    Datum       aclDatum;
    bool        isNull = false;
    Acl        *acl = NULL;
    Oid         ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege.*/
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Get the function's ACL from gs_column_keys
     */
    tuple = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(column_setting_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function with OID %u does not exist", column_setting_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }

    ownerId = ((Form_gs_column_keys) GETSTRUCT(tuple))->key_owner;

    aclDatum = SysCacheGetAttr(COLUMNSETTINGOID, tuple, Anum_gs_column_keys_key_acl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_COLUMN_SETTING, ownerId);
        aclDatum = (Datum) 0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    if (check_nodegroup) {
        check_nodegroup_privilege(roleid, ownerId, ACL_USAGE);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if (acl && (Pointer) acl != DatumGetPointer(aclDatum)) {
        pfree_ext(acl);
    }

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a largeobject
 *
 * When a large object is opened for reading, it is opened relative to the
 * caller's snapshot, but when it is opened for writing, it is always relative
 * to SnapshotNow, as documented in doc/src/sgml/lobj.sgml.  This function
 * takes a snapshot argument so that the permissions check can be made relative
 * to the same snapshot that will be used to read the underlying data.
 */
AclMode pg_largeobject_aclmask_snapshot(Oid lobj_oid, Oid roleid, AclMode mask, AclMaskHow how, Snapshot snapshot)
{
    AclMode result;
    Relation pg_lo_meta = NULL;
    ScanKeyData entry[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Get the largeobject's ACL from pg_language_metadata
     */
    pg_lo_meta = heap_open(LargeObjectMetadataRelationId, AccessShareLock);

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(lobj_oid));

    scan = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true, snapshot, 1, entry);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("large object %u does not exist", lobj_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_largeobject_metadata)GETSTRUCT(tuple))->lomowner;

    aclDatum = heap_getattr(tuple, Anum_pg_largeobject_metadata_lomacl, RelationGetDescr(pg_lo_meta), &isNull);

    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_LARGEOBJECT, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if (acl != NULL && (Pointer)acl != DatumGetPointer(aclDatum))
        pfree_ext(acl);

    systable_endscan(scan);

    heap_close(pg_lo_meta, AccessShareLock);

    return result;
}

/*
 * Exported routine for examining a user's privileges for blockchain namespace
 */
static AclMode gs_blockchain_aclmask(Oid roleid, AclMode mask)
{
    /* Only super user or audit admin have access right to blockchain nsp */
    if (superuser_arg(roleid) || isAuditadmin(roleid)) {
        return REMOVE_DDL_FLAG(mask);
    } else {
        return ACL_NO_RIGHTS; /* not allowed to get access right by granting. */
    }
}

static AclMode check_usage_privilege(Oid nsp_oid, AclMode mask, Oid roleid, AclMode result)
{
    if (!(mask & ACL_USAGE) || (result & ACL_USAGE)) {
        return result;
    }
    if (!IsSysSchema(nsp_oid) && HasOneOfAnyPriv(roleid)) {
        result |= ACL_USAGE;
    }
    return result;
}

/*
 * The initial user and operator admin in operation mode
 * can bypass permission check for schema pg_catalog.
*/
static bool is_pg_catalog_bypass_user(Oid roleid)
{
    return roleid == INITIAL_USER_ID || (isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode);
}

/*
 * Sysadmin and operator admin in operation mode
 * can bypass permission check for all schemas except for schema dbe_perf, snapshot and pg_catalog.
*/
static bool is_namespace_bypass_user(Oid roleid)
{
    return superuser_arg(roleid) || (isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode);
}

/*
 * Exported routine for examining a user's privileges for a namespace
 */
AclMode pg_namespace_aclmask(Oid nsp_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /* Only super user or audit admin have access right to blockchain nsp */
    if (nsp_oid == PG_BLOCKCHAIN_NAMESPACE) {
        return gs_blockchain_aclmask(roleid, mask);
    }

    /*
     * The initial user bypass all permission checking.
     * Sysadmin bypass all permission checking except for schema dbe_perf, snapshot and pg_catalog.
     * Monitoradmin can always bypass permission checking for schema dbe_perf and schema snapshot.
     */
    if (IsMonitorSpace(nsp_oid)) {
        if (isMonitoradmin(roleid) || roleid == INITIAL_USER_ID) {
            return REMOVE_DDL_FLAG(mask);
        }
    } else if (IsSystemNamespace(nsp_oid)) {
        if (is_pg_catalog_bypass_user(roleid)) {
            return REMOVE_DDL_FLAG(mask);
        }
    } else if (is_namespace_bypass_user(roleid)) {
        return REMOVE_DDL_FLAG(mask);
    }

    /*
     * If we have been assigned this namespace as a temp namespace, check to
     * make sure we have CREATE TEMP permission on the database, and if so act
     * as though we have all standard (but not GRANT OPTION) permissions on
     * the namespace.  If we don't have CREATE TEMP, act as though we have
     * only USAGE (and not CREATE) rights.
     *
     * This may seem redundant given the check in InitTempTableNamespace, but
     * it really isn't since current user ID may have changed since then. The
     * upshot of this behavior is that a SECURITY DEFINER function can create
     * temp tables that can then be accessed (if permission is granted) by
     * code in the same session that doesn't have permissions to create temp
     * tables.
     *
     * XXX Would it be safe to ereport a special error message as
     * InitTempTableNamespace does?  Returning zero here means we'll get a
     * generic "permission denied for schema pg_temp_N" message, which is not
     * remarkably user-friendly.
     */
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    if (isTempNamespace(nsp_oid) && !is_ddl_privileges) {
        if (pg_database_aclcheck(u_sess->proc_cxt.MyDatabaseId, roleid, ACL_CREATE_TEMP) == ACLCHECK_OK)
            return mask & ACL_ALL_RIGHTS_NAMESPACE;
        else
            return mask & ACL_USAGE;
    }

    /*
     * Get the schema's ACL from pg_namespace
     */
    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_SCHEMA),
            errmsg("schema with OID %u does not exist", nsp_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_namespace)GETSTRUCT(tuple))->nspowner;

    aclDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    if (check_nodegroup) {
        check_nodegroup_privilege(roleid, ownerId, mask);
    }

    if (IsMonitorSpace(nsp_oid) || IsSystemNamespace(nsp_oid)) {
        result = aclmask_without_sysadmin(acl, roleid, ownerId, mask, how);
    } else {
        result = aclmask(acl, roleid, ownerId, mask, how);
    }
    /*
     * Check if ACL_USAGE is being checked and, if so, and not set already as
     * part of the result, then check if the user has at least one of ANY privlege,
     * which allow usage access to all schemas except system schema.
     */
    result = check_usage_privilege(nsp_oid, mask, roleid, result);
    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * @Description :  Exported routine for examining a user's privileges for a node group
 * @in groupoid - node group oid
 * @in roleid - user oid
 * @in mask - privilege mode
 * @in how -  operation codes for pg_*_aclmask
 * @return - AclMode
 */
AclMode pg_nodegroup_aclmask(Oid group_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /* User with createdb bypass all permission checking. */
    bool is_createdb = false;
    HeapTuple utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup)) {
        is_createdb = ((Form_pg_authid)GETSTRUCT(utup))->rolcreatedb;
        ReleaseSysCache(utup);
    }
    if (is_createdb) {
        return REMOVE_DDL_FLAG(mask);
    }

    /*
     * Get the nodegroup's ACL from pgxc_group
     */
    tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("node group with OID %u does not exist", group_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = BOOTSTRAP_SUPERUSERID;

    isNull = true;

    aclDatum = SysCacheGetAttr(PGXCGROUPOID, tuple, Anum_pgxc_group_group_acl, &isNull);

    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_NODEGROUP, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a tablespace
 */
AclMode pg_tablespace_aclmask(Oid spc_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);
    
    if (isOperatoradmin(roleid) && u_sess->attr.attr_security.operation_mode) {
        return REMOVE_DDL_FLAG(mask);
    }

    /*
     * Get the tablespace's ACL from pg_tablespace
     */
    tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spc_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("tablespace with OID %u does not exist", spc_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_tablespace)GETSTRUCT(tuple))->spcowner;

    aclDatum = SysCacheGetAttr(TABLESPACEOID, tuple, Anum_pg_tablespace_spcacl, &isNull);

    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * data wrapper
 */
AclMode pg_foreign_data_wrapper_aclmask(Oid fdw_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    Form_pg_foreign_data_wrapper fdwForm;

    /* Bypass permission checks for superusers */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Must get the FDW's tuple from pg_foreign_data_wrapper
     */
    tuple = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdw_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("foreign-data wrapper with OID %u does not exist", fdw_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    fdwForm = (Form_pg_foreign_data_wrapper)GETSTRUCT(tuple);

    /*
     * Normal case: get the FDW's ACL from pg_foreign_data_wrapper
     */
    ownerId = fdwForm->fdwowner;

    aclDatum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tuple, Anum_pg_foreign_data_wrapper_fdwacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_FDW, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast rel's ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * server.
 */
AclMode pg_foreign_server_aclmask(Oid srv_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    Form_pg_foreign_server srvForm;

    /* Bypass permission checks for superusers */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Must get the FDW's tuple from pg_foreign_data_wrapper
     */
    tuple = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(srv_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("foreign server with OID %u does not exist", srv_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    srvForm = (Form_pg_foreign_server)GETSTRUCT(tuple);

    /*
     * Normal case: get the foreign server's ACL from pg_foreign_server
     */
    ownerId = srvForm->srvowner;

    aclDatum = SysCacheGetAttr(FOREIGNSERVEROID, tuple, Anum_pg_foreign_server_srvacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast rel's ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * pg_extension_data_source_aclmask
 * 	Exported routine for examining a user's privileges for a data source
 *
 * @src_oid: source oid
 * @roleid: role oid
 * @mask: ACL privileges mode
 * @how: the way to check
 */
AclMode pg_extension_data_source_aclmask(Oid src_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;
    Form_pg_extension_data_source srcForm;

    /*
     * Bypass permission checks for superusers
     * Database Security:  Support separation of privilege.
     */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /* Get data source tuple */
    tuple = SearchSysCache1(DATASOURCEOID, ObjectIdGetDatum(src_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("data source with OID %u does not exist", src_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    srcForm = (Form_pg_extension_data_source)GETSTRUCT(tuple);

    /* Normal case: get the data source's ACL from pg_extension_data_source */
    ownerId = srcForm->srcowner;
    aclDatum = SysCacheGetAttr(DATASOURCEOID, tuple, Anum_pg_extension_data_source_srcacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_DATA_SOURCE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast rel's ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* If we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for examining a user's privileges for a type.
 */
AclMode pg_type_aclmask(Oid type_oid, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    HeapTuple tuple = NULL;
    Datum aclDatum;
    bool isNull = false;
    Acl* acl = NULL;
    Oid ownerId;

    Form_pg_type typeForm;

    /* Bypass permission checks for superusers */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return REMOVE_DDL_FLAG(mask);

    /*
     * Must get the type's tuple from pg_type
     */
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("type with OID %u does not exist", type_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    typeForm = (Form_pg_type)GETSTRUCT(tuple);

    /*
     * "True" array types don't manage permissions of their own; consult the
     * element type instead.
     */
    if (OidIsValid(typeForm->typelem) && typeForm->typlen == -1) {
        Oid elttype_oid = typeForm->typelem;

        ReleaseSysCache(tuple);
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(elttype_oid));

        /* this case is not a user-facing error, so elog not ereport */
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u", elttype_oid), errdetail("N/A"),
                    errcause("System error."), erraction("Contact engineer to support.")));

        typeForm = (Form_pg_type)GETSTRUCT(tuple);
    }

    /*
     * Now get the type's owner and ACL from the tuple
     */
    ownerId = typeForm->typowner;
    aclDatum = SysCacheGetAttr(TYPEOID, tuple, Anum_pg_type_typacl, &isNull);
    if (isNull) {
        /* No ACL, so build default ACL */
        acl = acldefault(ACL_OBJECT_TYPE, ownerId);
        aclDatum = (Datum)0;
    } else {
        /* detoast rel's ACL if necessary */
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    FREE_DETOASTED_ACL(acl, aclDatum);

    if ((how == ACLMASK_ANY && result != 0) || IsSysSchema(typeForm->typnamespace)) {
        ReleaseSysCache(tuple);
        return result;
    }
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    if (is_ddl_privileges) {
        if ((REMOVE_DDL_FLAG(mask) & ACL_ALTER) && !(result & ACL_ALTER)) {
            if (HasSpecAnyPriv(roleid, ALTER_ANY_TYPE, false)) {
                result |= ACL_ALTER;
            }
        }
        if ((REMOVE_DDL_FLAG(mask) & ACL_DROP) && !(result & ACL_DROP)) {
            if (HasSpecAnyPriv(roleid, DROP_ANY_TYPE, false)) {
                result |= ACL_DROP;
            }
        }
    }

    ReleaseSysCache(tuple);

    return result;
}

/*
 * Exported routine for checking a user's access privileges to a column
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 *
 * As with pg_attribute_aclmask, only privileges granted directly on the
 * column are considered here.
 */
AclResult pg_attribute_aclcheck(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mode)
{
    if (pg_attribute_aclmask(table_oid, attnum, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to any/all columns
 *
 * If 'how' is ACLMASK_ANY, then returns ACLCHECK_OK if user has any of the
 * privileges identified by 'mode' on any non-dropped column in the relation;
 * otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 *
 * If 'how' is ACLMASK_ALL, then returns ACLCHECK_OK if user has any of the
 * privileges identified by 'mode' on each non-dropped column in the relation
 * (and there must be at least one such column); otherwise returns a suitable
 * error code (in practice, always ACLCHECK_NO_PRIV).
 *
 * As with pg_attribute_aclmask, only privileges granted directly on the
 * column(s) are considered here.
 *
 * Note: system columns are not considered here; there are cases where that
 * might be appropriate but there are also cases where it wouldn't.
 */
AclResult pg_attribute_aclcheck_all(Oid table_oid, Oid roleid, AclMode mode, AclMaskHow how)
{
    AclResult result;
    HeapTuple classTuple = NULL;
    Form_pg_class classForm = NULL;
    AttrNumber nattrs;
    AttrNumber curr_att;

    /*
     * Must fetch pg_class row to check number of attributes.  As in
     * pg_attribute_aclmask, we prefer to return "no privileges" instead of
     * throwing an error if we get any unexpected lookup errors.
     */
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if (!HeapTupleIsValid(classTuple))
        return ACLCHECK_NO_PRIV;
    classForm = (Form_pg_class)GETSTRUCT(classTuple);

    CatalogRelationBuildParam catalogParam = GetCatalogParam(table_oid);
    if (catalogParam.oid != InvalidOid) {
        nattrs = (AttrNumber)catalogParam.natts;
    } else {
        nattrs = classForm->relnatts;
    }

    ReleaseSysCache(classTuple);

    /*
     * Initialize result in case there are no non-dropped columns.	We want to
     * report failure in such cases for either value of 'how'.
     */
    result = ACLCHECK_NO_PRIV;

    for (curr_att = 1; curr_att <= nattrs; curr_att++) {
        HeapTuple attTuple = NULL;
        AclMode attmask;

        attTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(table_oid), Int16GetDatum(curr_att));
        if (!HeapTupleIsValid(attTuple))
            continue;

        /* ignore dropped columns */
        if (((Form_pg_attribute)GETSTRUCT(attTuple))->attisdropped) {
            ReleaseSysCache(attTuple);
            continue;
        }

        /*
         * Here we hard-wire knowledge that the default ACL for a column
         * grants no privileges, so that we can fall out quickly in the very
         * common case where attacl is null.
         */
        if (heap_attisnull(attTuple, Anum_pg_attribute_attacl, NULL))
            attmask = 0;
        else
            attmask = pg_attribute_aclmask(table_oid, curr_att, roleid, mode, ACLMASK_ANY);

        ReleaseSysCache(attTuple);

        if (attmask != 0) {
            result = ACLCHECK_OK;
            if (how == ACLMASK_ANY)
                break; /* succeed on any success */
        } else {
            result = ACLCHECK_NO_PRIV;
            if (how == ACLMASK_ALL)
                break; /* fail on any failure */
        }
    }

    return result;
}

/*
 * Exported routine for checking a user's access privileges to a table
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 */
AclResult pg_class_aclcheck(Oid table_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    if (pg_class_aclmask(table_oid, roleid, mode, ACLMASK_ANY, check_nodegroup) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a database
 */
AclResult pg_database_aclcheck(Oid db_oid, Oid roleid, AclMode mode)
{
    if (pg_database_aclmask(db_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a directory
 */
AclResult pg_directory_aclcheck(Oid dir_oid, Oid roleid, AclMode mode)
{
    if (pg_directory_aclmask(dir_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a function
 */
AclResult pg_proc_aclcheck(Oid proc_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    if (pg_proc_aclmask(proc_oid, roleid, mode, ACLMASK_ANY, check_nodegroup) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a package
 */
AclResult pg_package_aclcheck(Oid pkgOid, Oid roleid, AclMode mode, bool checkNodegroup)
{
    if (pg_package_aclmask(pkgOid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a client master key
 */
AclResult gs_sec_cmk_aclcheck(Oid key_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    if (gs_sec_cmk_aclmask(key_oid, roleid, mode, ACLMASK_ANY, check_nodegroup) != 0) {
        return ACLCHECK_OK;
    } else {
        return ACLCHECK_NO_PRIV;
    }
}

/*
 * Exported routine for checking a user's access privileges to a column encryption key
 */
AclResult gs_sec_cek_aclcheck(Oid key_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    if (gs_sec_cek_aclmask(key_oid, roleid, mode, ACLMASK_ANY, check_nodegroup) != 0) {
        return ACLCHECK_OK;
    } else {
        return ACLCHECK_NO_PRIV;
    }
}

/*
 * Exported routine for checking a user's access privileges to a language
 */
AclResult pg_language_aclcheck(Oid lang_oid, Oid roleid, AclMode mode)
{
    if (pg_language_aclmask(lang_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a largeobject
 */
AclResult pg_largeobject_aclcheck_snapshot(Oid lobj_oid, Oid roleid, AclMode mode, Snapshot snapshot)
{
    if (pg_largeobject_aclmask_snapshot(lobj_oid, roleid, mode, ACLMASK_ANY, snapshot) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * @Description :  Exported routine for checking a user's access privileges to a node group
 * @in groupoid - node group oid
 * @in roleid - user oid
 * @in mode - privilege mode
 * @return - AclResult
 */
AclResult pg_nodegroup_aclcheck(Oid group_oid, Oid roleid, AclMode mode)
{
    if (pg_nodegroup_aclmask(group_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a namespace
 */
AclResult pg_namespace_aclcheck(Oid nsp_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    if (pg_namespace_aclmask(nsp_oid, roleid, mode, ACLMASK_ANY, check_nodegroup) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a tablespace
 */
AclResult pg_tablespace_aclcheck(Oid spc_oid, Oid roleid, AclMode mode)
{
    if (pg_tablespace_aclmask(spc_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * data wrapper
 */
AclResult pg_foreign_data_wrapper_aclcheck(Oid fdw_oid, Oid roleid, AclMode mode)
{
    if (pg_foreign_data_wrapper_aclmask(fdw_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * server
 */
AclResult pg_foreign_server_aclcheck(Oid srv_oid, Oid roleid, AclMode mode)
{
    if (pg_foreign_server_aclmask(srv_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * pg_extension_data_source_aclcheck
 * 	Exported routine for checking a user's access privileges to a data source
 *
 * @src_oid: source oid
 * @roleid: role oid
 * @mode: ACL privileges mode
 */
AclResult pg_extension_data_source_aclcheck(Oid src_oid, Oid roleid, AclMode mode)
{
    if (pg_extension_data_source_aclmask(src_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a type
 */
AclResult pg_type_aclcheck(Oid type_oid, Oid roleid, AclMode mode)
{
    if (pg_type_aclmask(type_oid, roleid, mode, ACLMASK_ANY) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}

/*
 * Ownership check for a relation (specified by OID).
 */
bool pg_class_ownercheck(Oid class_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(class_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("relation with OID %u does not exist", class_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_class)GETSTRUCT(tuple))->relowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a type (specified by OID).
 */
bool pg_type_ownercheck(Oid type_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("type with OID %u does not exist", type_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_type)GETSTRUCT(tuple))->typowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator (specified by OID).
 */
bool pg_oper_ownercheck(Oid oper_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("operator with OID %u does not exist", oper_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_operator)GETSTRUCT(tuple))->oprowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a function (specified by OID).
 */
bool pg_proc_ownercheck(Oid proc_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function with OID %u does not exist", proc_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_proc)GETSTRUCT(tuple))->proowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a package (specified by OID).
 */
bool pg_package_ownercheck(Oid package_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(package_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_PACKAGE),
            errmsg("package with OID %u does not exist", package_oid), errdetail("N/A"),
                errcause("System error."), erraction("Re-entering the session")));

    ownerId = ((Form_gs_package)GETSTRUCT(tuple))->pkgowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}


/*
 * Ownership check for a client master key (specified by OID).
 */
bool gs_sec_cmk_ownercheck(Oid key_oid, Oid roleid)
{
    HeapTuple tuple;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid)) {
        return true;
    }
    tuple = SearchSysCache1(GLOBALSETTINGOID, ObjectIdGetDatum(key_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_KEY), 
            errmsg("client master key with OID %u does not exist", key_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    ownerId = ((Form_gs_client_global_keys)GETSTRUCT(tuple))->key_owner;
    ReleaseSysCache(tuple);
    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a function (specified by OID).
 */
bool gs_sec_cek_ownercheck(Oid key_oid, Oid roleid)
{
    HeapTuple tuple;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid)) {
        return true;
    }
    tuple = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(key_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_KEY),
            errmsg("column encryption key with OID %u does not exist", key_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    ownerId = ((Form_gs_column_keys)GETSTRUCT(tuple))->key_owner;
    ReleaseSysCache(tuple);
    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a procedural language (specified by OID)
 */
bool pg_language_ownercheck(Oid lan_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(lan_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("language with OID %u does not exist", lan_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_language)GETSTRUCT(tuple))->lanowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a largeobject (specified by OID)
 *
 * This is only used for operations like ALTER LARGE OBJECT that are always
 * relative to SnapshotNow.
 */
bool pg_largeobject_ownercheck(Oid lobj_oid, Oid roleid)
{
    Relation pg_lo_meta = NULL;
    ScanKeyData entry[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    /* There's no syscache for pg_largeobject_metadata */
    pg_lo_meta = heap_open(LargeObjectMetadataRelationId, AccessShareLock);

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(lobj_oid));

    scan = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true, NULL, 1, entry);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("large object %u does not exist", lobj_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_largeobject_metadata)GETSTRUCT(tuple))->lomowner;

    systable_endscan(scan);
    heap_close(pg_lo_meta, AccessShareLock);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a namespace (specified by OID).
 */
bool pg_namespace_ownercheck(Oid nsp_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid))
        return true;

    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_SCHEMA),
            errmsg("schema with OID %u does not exist", nsp_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_namespace)GETSTRUCT(tuple))->nspowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a tablespace (specified by OID).
 */
bool pg_tablespace_ownercheck(Oid spc_oid, Oid roleid)
{
    HeapTuple spctuple = NULL;
    Oid spcowner;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    /* Search syscache for pg_tablespace */
    spctuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spc_oid));
    if (!HeapTupleIsValid(spctuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("tablespace with OID %u does not exist", spc_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    spcowner = ((Form_pg_tablespace)GETSTRUCT(spctuple))->spcowner;

    ReleaseSysCache(spctuple);

    return has_privs_of_role(roleid, spcowner);
}

/*
 * Ownership check for an operator class (specified by OID).
 */
bool pg_opclass_ownercheck(Oid opc_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(opc_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("operator class with OID %u does not exist", opc_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_opclass)GETSTRUCT(tuple))->opcowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator family (specified by OID).
 */
bool pg_opfamily_ownercheck(Oid opf_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opf_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("operator family with OID %u does not exist", opf_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_opfamily)GETSTRUCT(tuple))->opfowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a text search dictionary (specified by OID).
 */
bool pg_ts_dict_ownercheck(Oid dict_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dict_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("text search dictionary with OID %u does not exist", dict_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_ts_dict)GETSTRUCT(tuple))->dictowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a text search configuration (specified by OID).
 */
bool pg_ts_config_ownercheck(Oid cfg_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfg_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("text search configuration with OID %u does not exist", cfg_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_ts_config)GETSTRUCT(tuple))->cfgowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a foreign-data wrapper (specified by OID).
 */
bool pg_foreign_data_wrapper_ownercheck(Oid srv_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(srv_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("foreign-data wrapper with OID %u does not exist", srv_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_foreign_data_wrapper)GETSTRUCT(tuple))->fdwowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a foreign server (specified by OID).
 */
bool pg_foreign_server_ownercheck(Oid srv_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(srv_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("foreign server with OID %u does not exist", srv_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_foreign_server)GETSTRUCT(tuple))->srvowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an event trigger (specified by OID).
 */
bool pg_event_trigger_ownercheck(Oid et_oid, Oid roleid)
{
    HeapTuple   tuple;
    Oid         ownerId;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid))
        return true;

    tuple = SearchSysCache1(EVENTTRIGGEROID, ObjectIdGetDatum(et_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("event trigger with OID %u does not exist",
                        et_oid)));

    ownerId = ((Form_pg_event_trigger) GETSTRUCT(tuple))->evtowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a database (specified by OID).
 */
bool pg_database_ownercheck(Oid db_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid dba;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(db_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("database with OID %u does not exist", db_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    dba = ((Form_pg_database)GETSTRUCT(tuple))->datdba;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, dba);
}

/*
 * Ownership check for a directory (specified by OID).
 */
bool pg_directory_ownercheck(Oid dir_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid dba;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(DIRECTORYOID, ObjectIdGetDatum(dir_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("directory with OID %u does not exist", dir_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    dba = ((Form_pg_directory)GETSTRUCT(tuple))->owner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, dba);
}

/*
 * Ownership check for a collation (specified by OID).
 */
bool pg_collation_ownercheck(Oid coll_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege.*/
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(coll_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("collation with OID %u does not exist", coll_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_collation)GETSTRUCT(tuple))->collowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a conversion (specified by OID).
 */
bool pg_conversion_ownercheck(Oid conv_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    tuple = SearchSysCache1(CONVOID, ObjectIdGetDatum(conv_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("conversion with OID %u does not exist", conv_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_conversion)GETSTRUCT(tuple))->conowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an extension (specified by OID).
 */
bool pg_extension_ownercheck(Oid ext_oid, Oid roleid)
{
    Relation pg_extension = NULL;
    ScanKeyData entry[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    /* Database Security:  Support separation of privilege. */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    /* There's no syscache for pg_extension, so do it the hard way */
    pg_extension = heap_open(ExtensionRelationId, AccessShareLock);

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ext_oid));

    scan = systable_beginscan(pg_extension, ExtensionOidIndexId, true, NULL, 1, entry);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("extension with OID %u does not exist", ext_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_extension)GETSTRUCT(tuple))->extowner;

    systable_endscan(scan);
    heap_close(pg_extension, AccessShareLock);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * pg_extension_data_source_ownercheck
 * 	Ownership check for a data source (specified by OID).
 *
 * @src_oid: source oid
 * @roleid: role oid
 */
bool pg_extension_data_source_ownercheck(Oid src_oid, Oid roleid)
{
    HeapTuple tuple = NULL;
    Oid ownerId;

    /* Superusers bypass all permission checking */
    if (superuser_arg(roleid) || systemDBA_arg(roleid))
        return true;

    /* Search the data source by oid */
    tuple = SearchSysCache1(DATASOURCEOID, ObjectIdGetDatum(src_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("data source with OID %u does not exist", src_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    ownerId = ((Form_pg_extension_data_source)GETSTRUCT(tuple))->srcowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * pg_synonym_ownercheck
 * 	Ownership check for a synonym (specified by OID).
 *
 * @synOid: synonym oid
 * @roleId: role oid
 */
bool pg_synonym_ownercheck(Oid synOid, Oid roleId)
{
    HeapTuple tuple;
    Oid ownerId;

    /* Superusers bypass all permission checking */
    if (superuser_arg(roleId) || systemDBA_arg(roleId)) {
        return true;
    }

    /* Search the data source by oid */
    tuple = SearchSysCache1(SYNOID, ObjectIdGetDatum(synOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("synonym with OID %u does not exist", synOid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }

    ownerId = ((Form_pg_synonym)GETSTRUCT(tuple))->synowner;
    ReleaseSysCache(tuple);

    return has_privs_of_role(roleId, ownerId);
}

/*
 * Ownership check for an publication (specified by OID).
 */
bool pg_publication_ownercheck(Oid pub_oid, Oid roleid)
{
    HeapTuple tuple;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid)) {
        return true;
    }

    tuple = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pub_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("publication with OID %u does not exist", pub_oid)));
    }

    ownerId = ((Form_pg_publication)GETSTRUCT(tuple))->pubowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an subscription (specified by OID).
 */
bool pg_subscription_ownercheck(Oid sub_oid, Oid roleid)
{
    HeapTuple tuple;
    Oid ownerId;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid)) {
        return true;
    }

    tuple = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(sub_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription with OID %u does not exist", sub_oid)));
    }

    ownerId = ((Form_pg_subscription)GETSTRUCT(tuple))->subowner;

    ReleaseSysCache(tuple);

    return has_privs_of_role(roleid, ownerId);
}

/*
 * Check whether specified role has CREATEROLE privilege (or is a superuser)
 *
 * Note: roles do not have owners per se; instead we use this test in
 * places where an ownership-like permissions test is needed for a role.
 * Be sure to apply it to the role trying to do the operation, not the
 * role being operated on!	Also note that this generally should not be
 * considered enough privilege if the target role is a superuser.
 * (We don't handle that consideration here because we want to give a
 * separate error message for such cases, so the caller has to deal with it.)
 */
bool has_createrole_privilege(Oid roleid)
{
    bool result = false;
    HeapTuple utup = NULL;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid)) {
        return true;
    }

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup)) {
        result = ((Form_pg_authid)GETSTRUCT(utup))->rolcreaterole;
        ReleaseSysCache(utup);
    }
    return result;
}

/* Database Security: Support database audit */
/*
 * Brief		: Check whether specified role has AUDITADMIN privilege (or is a superuser)
 * Description	:
 */
bool has_auditadmin_privilege(Oid roleid)
{
    bool result = false;
    HeapTuple utup = NULL;

    /* Superusers bypass all permission checking. */
    if (superuser_arg(roleid))
        return true;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup)) {
        result = ((Form_pg_authid)GETSTRUCT(utup))->rolauditadmin;
        ReleaseSysCache(utup);
    }
    return result;
}

/*
 * Fetch pg_default_acl entry for given role, namespace and object type
 * (object type must be given in pg_default_acl's encoding).
 * Returns NULL if no such entry.
 */
static Acl* get_default_acl_internal(Oid roleId, Oid nsp_oid, char objtype)
{
    Acl* result = NULL;
    HeapTuple tuple = NULL;

    tuple =
        SearchSysCache3(DEFACLROLENSPOBJ, ObjectIdGetDatum(roleId), ObjectIdGetDatum(nsp_oid), CharGetDatum(objtype));

    if (HeapTupleIsValid(tuple)) {
        Datum aclDatum;
        bool isNull = false;

        aclDatum = SysCacheGetAttr(DEFACLROLENSPOBJ, tuple, Anum_pg_default_acl_defaclacl, &isNull);
        if (!isNull)
            result = DatumGetAclPCopy(aclDatum);
        ReleaseSysCache(tuple);
    }

    return result;
}

/*
 * Get default permissions for newly created object within given schema
 *
 * Returns NULL if built-in system defaults should be used
 */
Acl* get_user_default_acl(GrantObjectType objtype, Oid ownerId, Oid nsp_oid)
{
    Acl* result = NULL;
    Acl* glob_acl = NULL;
    Acl* schema_acl = NULL;
    Acl* def_acl = NULL;
    char defaclobjtype;

    /*
     * Use NULL during bootstrap, since pg_default_acl probably isn't there
     * yet.
     */
    if (IsBootstrapProcessingMode())
        return NULL;

    /* Check if object type is supported in pg_default_acl */
    switch (objtype) {
        case ACL_OBJECT_RELATION:
            defaclobjtype = DEFACLOBJ_RELATION;
            break;

        case ACL_OBJECT_SEQUENCE:
            defaclobjtype = DEFACLOBJ_SEQUENCE;
            break;

        case ACL_OBJECT_FUNCTION:
            defaclobjtype = DEFACLOBJ_FUNCTION;
            break;

        case ACL_OBJECT_TYPE:
            defaclobjtype = DEFACLOBJ_TYPE;
            break;

        case ACL_OBJECT_GLOBAL_SETTING:
            defaclobjtype = DEFACLOBJ_GLOBAL_SETTING;
            break;

        case ACL_OBJECT_COLUMN_SETTING:
            defaclobjtype = DEFACLOBJ_COLUMN_SETTING;
            break;

        case ACL_OBJECT_PACKAGE:
            defaclobjtype = DEFACLOBJ_PACKAGE;
            break;

        default:
            return NULL;
    }

    /* Look up the relevant pg_default_acl entries */
    glob_acl = get_default_acl_internal(ownerId, InvalidOid, defaclobjtype);
    schema_acl = get_default_acl_internal(ownerId, nsp_oid, defaclobjtype);

    /* Quick out if neither entry exists */
    if (glob_acl == NULL && schema_acl == NULL)
        return NULL;

    /* We need to know the hard-wired default value, too */
    def_acl = acldefault(objtype, ownerId);

    /* If there's no global entry, substitute the hard-wired default */
    if (glob_acl == NULL)
        glob_acl = def_acl;

    /* Merge in any per-schema privileges */
    result = aclmerge(glob_acl, schema_acl, ownerId);

    /*
     * For efficiency, we want to return NULL if the result equals default.
     * This requires sorting both arrays to get an accurate comparison.
     */
    aclitemsort(result);
    aclitemsort(def_acl);
    if (aclequal(result, def_acl))
        result = NULL;

    return result;
}

/*
 * @Description: check sysadmin's privilege for independent role's table.
 * @in mask : the acl mask information.
 * @in relkind : the relkind of the object be checked.
 * @return : false for independent and true for noindependent.
 */
bool independent_priv_aclcheck(AclMode mask, char relkind)
{
    /*
     * When sysadmin access independent role's table, INSERT/DELETE/SELECT/UPDATE/COPY
     * are forbidden without independent role active authorization.
     * Correspondence between operation and privielges are:
     * INSERT	-->		ACL_INSERT
     * DELETE	-->		ACL_DELETE
     * SELECT	-->		ACL_SELECT
     * UPDATE	-->		ACL_UPDATE
     * COPY		-->		ACL_SELECT
     */
    if (mask & (ACL_INSERT | ACL_UPDATE)) {
        return false;
    } else if (mask & (ACL_SELECT | ACL_DELETE)) {
        /*
         * Redis use the SELECT and DELETE for all table.
         * Redistribution of data : INSERT INTO temp_tbl SELECT * FROM tablex
         * Online redistribution : DELETE FROM ONLY tablex where xxx IN(select * from pgxc_bucket)
         */
        if (u_sess->proc_cxt.clientIsGsredis && ClusterResizingInProgress()) {
            return true;
        } else if (u_sess->proc_cxt.clientIsGsdump && RELKIND_IS_SEQUENCE(relkind)) {
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
}

/*
 * @Description: check whether language is trusted.
 * @in lang_oid : oid of the language which need be check.
 * @return : true for trust and false for untrust.
 */
bool is_trust_language(Oid lang_oid)
{
    HeapTuple tuple;
    bool is_trust_lang = false;
    /* Get the language's lanpltrusted flag from pg_language. */
    tuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(lang_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("language with OID %u does not exist", lang_oid), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));

    is_trust_lang = ((Form_pg_language)GETSTRUCT(tuple))->lanpltrusted;
    ReleaseSysCache(tuple);

    return is_trust_lang;
}
