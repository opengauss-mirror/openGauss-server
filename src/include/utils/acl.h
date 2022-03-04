/* -------------------------------------------------------------------------
 *
 * acl.h
 *	  Definition of (and support for) access control list data structures.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/acl.h
 *
 * NOTES
 *	  An ACL array is simply an array of AclItems, representing the union
 *	  of the privileges represented by the individual items.  A zero-length
 *	  array represents "no privileges".  There are no assumptions about the
 *	  ordering of the items, but we do expect that there are no two entries
 *	  in the array with the same grantor and grantee.
 *
 *	  For backward-compatibility purposes we have to allow null ACL entries
 *	  in system catalogs.  A null ACL will be treated as meaning "default
 *	  protection" (i.e., whatever acldefault() returns).
 * -------------------------------------------------------------------------
 */
#ifndef ACL_H
#define ACL_H

#include "nodes/parsenodes.h"
#include "utils/array.h"
#include "utils/guc.h"
#include "utils/snapshot.h"

/*
 * typedef AclMode is declared in parsenodes.h, also the individual privilege
 * bit meanings are defined there
 */

#define ACL_ID_PUBLIC 0 /* placeholder for id in a PUBLIC acl item */

/*
 * AclItem
 *
 * Note: must be same size on all platforms, because the size is hardcoded
 * in the pg_type.h entry for aclitem.
 */
typedef struct AclItem {
    Oid ai_grantee;   /* ID that this item grants privs to */
    Oid ai_grantor;   /* grantor of privs */
    AclMode ai_privs; /* privilege bits */
} AclItem;

/* type of privileges */
typedef enum { DML_PRIVS_INDEX = 0, DDL_PRIVS_INDEX } PRIVS_INDEX;
#define PRIVS_ATTR_NUM 2

/*
 * The upper 16 bits of the ai_privs field of an AclItem are the grant option
 * bits, and the lower 16 bits are the actual privileges.  We use "rights"
 * to mean the combined grant option and privilege bits fields.
 */
#define ACLITEM_GET_PRIVS(item) ((item).ai_privs & 0xFFFF)
#define ACLITEM_GET_GOPTIONS(item) (((item).ai_privs >> 16) & 0xFFFF)
#define ACLITEM_GET_RIGHTS(item) ((item).ai_privs)

#define ACL_GRANT_OPTION_FOR(privs) (((AclMode)(privs)&0xFFFF) << 16)
#define ACL_OPTION_TO_PRIVS(privs) (((AclMode)(privs) >> 16) & 0xFFFF)

#define ACLMODE_FOR_DDL(privs) (((privs) & FLAG_FOR_DDL_ACL) == 0 ? false : true)
/* remove ddl privileges flag from Aclitem */
#define REMOVE_DDL_FLAG(privs) ((privs) & (~((AclMode)FLAG_FOR_DDL_ACL & 0xFFFF)))
/* add ddl privileges flag to Aclitem */
#define ADD_DDL_FLAG(privs) ((privs) | ((AclMode)FLAG_FOR_DDL_ACL & 0xFFFF))

#define ACLITEM_SET_PRIVS(item, privs) \
    ((item).ai_privs = ((item).ai_privs & ~((AclMode)0xFFFF)) | ((AclMode)(privs)&0xFFFF))
#define ACLITEM_SET_GOPTIONS(item, goptions) \
    ((item).ai_privs = ((item).ai_privs & ~(((AclMode)0xFFFF) << 16)) | (((AclMode)(goptions)&0xFFFF) << 16))
#define ACLITEM_SET_RIGHTS(item, rights) ((item).ai_privs = (AclMode)(rights))

#define ACLITEM_SET_PRIVS_GOPTIONS(item, privs, goptions) \
    ((item).ai_privs = ((AclMode)(privs)&0xFFFF) | (((AclMode)(goptions)&0xFFFF) << 16))

#define ACLITEM_ALL_PRIV_BITS ((AclMode)0xFFFF)
#define ACLITEM_ALL_GOPTION_BITS ((AclMode)0xFFFF << 16)

/*
 * Definitions for convenient access to Acl (array of AclItem).
 * These are standard openGauss arrays, but are restricted to have one
 * dimension and no nulls.	We also ignore the lower bound when reading,
 * and set it to one when writing.
 *
 * CAUTION: as of PostgreSQL 7.1, these arrays are toastable (just like all
 * other array types).	Therefore, be careful to detoast them with the
 * macros provided, unless you know for certain that a particular array
 * can't have been toasted.
 */

/*
 * Acl			a one-dimensional array of AclItem
 */
typedef ArrayType Acl;

#define ACL_NUM(ACL) (ARR_DIMS(ACL)[0])
#define ACL_DAT(ACL) ((AclItem*)ARR_DATA_PTR(ACL))
#define ACL_N_SIZE(N) (ARR_OVERHEAD_NONULLS(1) + ((N) * sizeof(AclItem)))
#define ACL_SIZE(ACL) ARR_SIZE(ACL)

/*
 * fmgr macros for these types
 */
#define DatumGetAclItemP(X) ((AclItem*)DatumGetPointer(X))
#define PG_GETARG_ACLITEM_P(n) DatumGetAclItemP(PG_GETARG_DATUM(n))
#define PG_RETURN_ACLITEM_P(x) PG_RETURN_POINTER(x)

#define DatumGetAclP(X) ((Acl*)PG_DETOAST_DATUM(X))
#define DatumGetAclPCopy(X) ((Acl*)PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_ACL_P(n) DatumGetAclP(PG_GETARG_DATUM(n))
#define PG_GETARG_ACL_P_COPY(n) DatumGetAclPCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_ACL_P(x) PG_RETURN_POINTER(x)

/*
 * ACL modification opcodes for aclupdate
 */
#define ACL_MODECHG_ADD 1
#define ACL_MODECHG_DEL 2
#define ACL_MODECHG_EQL 3

/*
 * External representations of the privilege bits --- aclitemin/aclitemout
 * represent each possible privilege bit with a distinct 1-character code
 */
#define ACL_INSERT_CHR 'a' /* formerly known as "append" */
#define ACL_SELECT_CHR 'r' /* formerly known as "read" */
#define ACL_UPDATE_CHR 'w' /* formerly known as "write" */
#define ACL_DELETE_CHR 'd'
#define ACL_TRUNCATE_CHR 'D' /* super-delete, as it were */
#define ACL_REFERENCES_CHR 'x'
#define ACL_TRIGGER_CHR 't'
#define ACL_EXECUTE_CHR 'X'
#define ACL_USAGE_CHR 'U'
#define ACL_CREATE_CHR 'C'
#define ACL_CREATE_TEMP_CHR 'T'
#define ACL_CONNECT_CHR 'c'
#define ACL_COMPUTE_CHR 'p'
#define ACL_READ_CHR 'R'
#define ACL_WRITE_CHR 'W'

/* string holding all privilege code chars, in order by bitmask position */
#define ACL_ALL_RIGHTS_STR "arwdDxtXUCTcpRW"

/*
 * Bitmasks defining "all rights" for each supported object type
 */
#define ACL_ALL_RIGHTS_COLUMN (ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_REFERENCES)
#define ACL_ALL_RIGHTS_RELATION \
    (ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_REFERENCES | ACL_TRIGGER)
#define ACL_ALL_RIGHTS_SEQUENCE (ACL_USAGE | ACL_SELECT | ACL_UPDATE)
#define ACL_ALL_RIGHTS_DATABASE (ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT)
#define ACL_ALL_RIGHTS_FDW (ACL_USAGE)
#define ACL_ALL_RIGHTS_FOREIGN_SERVER (ACL_USAGE)
#define ACL_ALL_RIGHTS_FUNCTION (ACL_EXECUTE)
#define ACL_ALL_RIGHTS_PACKAGE (ACL_EXECUTE)
#define ACL_ALL_RIGHTS_LANGUAGE (ACL_USAGE)
#define ACL_ALL_RIGHTS_LARGEOBJECT (ACL_SELECT | ACL_UPDATE)
#define ACL_ALL_RIGHTS_NAMESPACE (ACL_USAGE | ACL_CREATE)
#define ACL_ALL_RIGHTS_NODEGROUP (ACL_CREATE | ACL_USAGE | ACL_COMPUTE)
#define ACL_ALL_RIGHTS_TABLESPACE (ACL_CREATE)
#define ACL_ALL_RIGHTS_TYPE (ACL_USAGE)
#define ACL_ALL_RIGHTS_DATA_SOURCE (ACL_USAGE)
#define ACL_ALL_RIGHTS_DIRECTORY (ACL_READ | ACL_WRITE)
#define ACL_ALL_RIGHTS_KEY (ACL_USAGE)

/* External representations of the ddl privilege bits --- aclitemin/aclitemout */
#define ACL_ALTER_CHR 'A' /* known as "modify" */
#define ACL_DROP_CHR 'P' /* known as "remove" */
#define ACL_COMMENT_CHR 'm' /* known as "statement" */
#define ACL_INDEX_CHR 'i'
#define ACL_VACUUM_CHR 'v'

/* string holding all ddl privilege code chars, in order by bitmask position */
#define ACL_ALL_DDL_RIGHTS_STR "APmiv"

/* Bitmasks defining "all ddl rights" for each supported object type */
#define ACL_ALL_DDL_RIGHTS_COLUMN (ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_RELATION (ACL_ALTER | ACL_DROP | ACL_COMMENT | ACL_INDEX | ACL_VACUUM)
#define ACL_ALL_DDL_RIGHTS_VIEW (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_SEQUENCE (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_NODEGROUP (ACL_ALTER | ACL_DROP)
#define ACL_ALL_DDL_RIGHTS_DATABASE (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_TABLESPACE (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_NAMESPACE (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_FUNCTION (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_PACKAGE  (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_TYPE (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_FOREIGN_SERVER (ACL_ALTER | ACL_DROP | ACL_COMMENT)
#define ACL_ALL_DDL_RIGHTS_FDW (ACL_NO_DDL_RIGHTS)
#define ACL_ALL_DDL_RIGHTS_DATA_SOURCE (ACL_NO_DDL_RIGHTS)
#define ACL_ALL_DDL_RIGHTS_DIRECTORY (ACL_ALTER | ACL_DROP)
#define ACL_ALL_DDL_RIGHTS_LARGEOBJECT (ACL_NO_DDL_RIGHTS)
#define ACL_ALL_DDL_RIGHTS_LANGUAGE (ACL_NO_DDL_RIGHTS)
#define ACL_ALL_DDL_RIGHTS_DOMAIN (ACL_NO_DDL_RIGHTS)
#define ACL_ALL_DDL_RIGHTS_KEY (ACL_DROP)

/* operation codes for pg_*_aclmask */
typedef enum {
    ACLMASK_ALL, /* normal case: compute all bits */
    ACLMASK_ANY  /* return when result is known nonzero */
} AclMaskHow;

/* result codes for pg_*_aclcheck */
typedef enum { ACLCHECK_OK = 0, ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER } AclResult;

typedef struct {
    const char* name;
    AclMode value;
} priv_map;

/* this enum covers all object types that can have privilege errors */
/* currently it's only used to tell aclcheck_error what to say */
typedef enum AclObjectKind {
    ACL_KIND_COLUMN,          /* pg_attribute */
    ACL_KIND_CLASS,           /* pg_class */
    ACL_KIND_SEQUENCE,        /* pg_sequence */
    ACL_KIND_DATABASE,        /* pg_database */
    ACL_KIND_PROC,            /* pg_proc */
    ACL_KIND_OPER,            /* pg_operator */
    ACL_KIND_TYPE,            /* pg_type */
    ACL_KIND_LANGUAGE,        /* pg_language */
    ACL_KIND_LARGEOBJECT,     /* pg_largeobject */
    ACL_KIND_NAMESPACE,       /* pg_namespace */
    ACL_KIND_NODEGROUP,       /* pgxc_group */
    ACL_KIND_OPCLASS,         /* pg_opclass */
    ACL_KIND_OPFAMILY,        /* pg_opfamily */
    ACL_KIND_COLLATION,       /* pg_collation */
    ACL_KIND_CONVERSION,      /* pg_conversion */
    ACL_KIND_TABLESPACE,      /* pg_tablespace */
    ACL_KIND_TSDICTIONARY,    /* pg_ts_dict */
    ACL_KIND_TSCONFIGURATION, /* pg_ts_config */
    ACL_KIND_FDW,             /* pg_foreign_data_wrapper */
    ACL_KIND_FOREIGN_SERVER,  /* pg_foreign_server */
    ACL_KIND_EXTENSION,       /* pg_extension */
    ACL_KIND_DATA_SOURCE,     /* data source */
    ACL_KIND_DIRECTORY,       /* pg_directory */
    ACL_KIND_COLUMN_SETTING,  /* column setting */
    ACL_KIND_GLOBAL_SETTING,  /* master client key */
    ACL_KIND_PACKAGE,         /* pg_package */
    ACL_KIND_PUBLICATION,     /* pg_publication */
    ACL_KIND_SUBSCRIPTION,    /* pg_subscription */
    MAX_ACL_KIND              /* MUST BE LAST */
} AclObjectKind;

/*
 * routines used internally
 */
extern Acl* acldefault(GrantObjectType objtype, Oid ownerId, Oid objId = InvalidOid);
extern Acl* get_user_default_acl(GrantObjectType objtype, Oid ownerId, Oid nsp_oid);

extern Acl* aclupdate(const Acl* old_acl, const AclItem* mod_aip, int modechg, Oid ownerId, DropBehavior behavior);
extern Acl* aclnewowner(const Acl* old_acl, Oid oldOwnerId, Oid newOwnerId);
extern Acl* make_empty_acl(void);
extern Acl* aclcopy(const Acl* orig_acl);
extern Acl* aclconcat(const Acl* left_acl, const Acl* right_acl);
extern Acl* aclmerge(const Acl* left_acl, const Acl* right_acl, Oid ownerId);
extern void aclitemsort(Acl* acl);
extern bool aclequal(const Acl* left_acl, const Acl* right_acl);

extern AclMode aclmask(const Acl* acl, Oid roleid, Oid ownerId, AclMode mask, AclMaskHow how);
extern AclMode aclmask_without_sysadmin(const Acl* acl, Oid roleid, Oid ownerId, AclMode mask, AclMaskHow how);
extern int aclmembers(const Acl* acl, Oid** roleids);

extern bool has_privs_of_role(Oid member, Oid role);
extern bool is_member_of_role(Oid member, Oid role);
extern bool is_member_of_role_nosuper(Oid member, Oid role);
extern bool is_admin_of_role(Oid member, Oid role);
extern void check_is_member_of_role(Oid member, Oid role);
extern Oid get_role_oid(const char* rolname, bool missing_ok);

extern void select_best_grantor(
    Oid roleId, AclMode privileges, AclMode ddl_privileges, const Acl* acl, Oid ownerId,
        Oid* grantorId, AclMode* grantOptions, AclMode* ddl_grantOptions, bool is_dbe_perf = false,
            bool isPgCatalog = false);
extern List * roles_has_privs_of(Oid roleid);
extern void initialize_acl(void);

/*
 * SQL functions (from acl.c)
 */
extern Datum aclitemin(PG_FUNCTION_ARGS);
extern Datum aclitemout(PG_FUNCTION_ARGS);
extern Datum aclinsert(PG_FUNCTION_ARGS);
extern Datum aclremove(PG_FUNCTION_ARGS);
extern Datum aclcontains(PG_FUNCTION_ARGS);
extern Datum makeaclitem(PG_FUNCTION_ARGS);
extern Datum aclitem_eq(PG_FUNCTION_ARGS);
extern Datum hash_aclitem(PG_FUNCTION_ARGS);
extern Datum acldefault_sql(PG_FUNCTION_ARGS);
extern Datum aclexplode(PG_FUNCTION_ARGS);

/*
 * prototypes for functions in aclchk.c
 */
extern List* GrantStmtGetObjectOids(GrantStmt* stmt);
extern void ExecuteGrantStmt(GrantStmt* stmt);
extern void ExecAlterDefaultPrivilegesStmt(AlterDefaultPrivilegesStmt* stmt);

extern void RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objid);
extern void RemoveDefaultACLById(Oid defaclOid);

extern AclMode pg_attribute_aclmask(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_class_aclmask(Oid table_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup = true);
extern AclMode pg_database_aclmask(Oid db_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_directory_aclmask(Oid dir_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_proc_aclmask(Oid proc_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup = true);
extern AclMode pg_package_aclmask(Oid packageOid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_language_aclmask(Oid lang_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_largeobject_aclmask_snapshot(
    Oid lobj_oid, Oid roleid, AclMode mask, AclMaskHow how, Snapshot snapshot);
extern AclMode pg_namespace_aclmask(Oid nsp_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup = true);
extern AclMode pg_nodegroup_aclmask(Oid group_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_tablespace_aclmask(Oid spc_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_foreign_data_wrapper_aclmask(Oid fdw_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_foreign_server_aclmask(Oid srv_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_extension_data_source_aclmask(Oid src_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode pg_type_aclmask(Oid type_oid, Oid roleid, AclMode mask, AclMaskHow how);
extern AclMode gs_sec_cmk_aclmask(Oid key_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup = true);
extern AclMode gs_sec_cek_aclmask(Oid key_oid, Oid roleid, AclMode mask, AclMaskHow how, bool check_nodegroup = true);

extern AclResult pg_attribute_aclcheck(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mode);
extern AclResult pg_attribute_aclcheck_all(Oid table_oid, Oid roleid, AclMode mode, AclMaskHow how);
extern AclResult pg_class_aclcheck(Oid table_oid, Oid roleid, AclMode mode, bool check_nodegroup = true);
extern AclResult pg_database_aclcheck(Oid db_oid, Oid roleid, AclMode mode);
extern AclResult pg_directory_aclcheck(Oid dir_oid, Oid roleid, AclMode mode);
extern AclResult pg_proc_aclcheck(Oid proc_oid, Oid roleid, AclMode mode, bool check_nodegroup = true);
extern AclResult pg_package_aclcheck(Oid pkgOid, Oid roleid, AclMode mode, bool checkNodegroup = true);
extern AclResult pg_language_aclcheck(Oid lang_oid, Oid roleid, AclMode mode);
extern AclResult pg_largeobject_aclcheck_snapshot(Oid lang_oid, Oid roleid, AclMode mode, Snapshot snapshot);
extern AclResult pg_namespace_aclcheck(Oid nsp_oid, Oid roleid, AclMode mode, bool check_nodegroup = true);
extern AclResult pg_nodegroup_aclcheck(Oid group_oid, Oid roleid, AclMode mode);
extern AclResult pg_tablespace_aclcheck(Oid spc_oid, Oid roleid, AclMode mode);
extern AclResult pg_foreign_data_wrapper_aclcheck(Oid fdw_oid, Oid roleid, AclMode mode);
extern AclResult pg_foreign_server_aclcheck(Oid srv_oid, Oid roleid, AclMode mode);
extern AclResult pg_extension_data_source_aclcheck(Oid src_oid, Oid roleid, AclMode mode);
extern AclResult pg_type_aclcheck(Oid type_oid, Oid roleid, AclMode mode);
extern AclResult gs_sec_cmk_aclcheck(Oid key_oid, Oid roleid, AclMode mode, bool check_nodegroup = false);
extern AclResult gs_sec_cek_aclcheck(Oid key_oid, Oid roleid, AclMode mode, bool check_nodegroup = false);

extern void aclcheck_error(AclResult aclerr, AclObjectKind objectkind, const char* objectname);
extern void aclcheck_error_msg(AclResult aclerr, AclObjectKind objectkind, const char *objectname, char *errstr,
    int size);
extern void aclcheck_error_col(AclResult aclerr, AclObjectKind objectkind, const char* objectname, const char* colname);
extern void aclcheck_error_type(AclResult aclerr, Oid typeOid);

/* ownercheck routines just return true (owner) or false (not) */
extern bool pg_class_ownercheck(Oid class_oid, Oid roleid);
extern bool pg_type_ownercheck(Oid type_oid, Oid roleid);
extern bool pg_oper_ownercheck(Oid oper_oid, Oid roleid);
extern bool pg_proc_ownercheck(Oid proc_oid, Oid roleid);
extern bool pg_language_ownercheck(Oid lan_oid, Oid roleid);
extern bool pg_largeobject_ownercheck(Oid lobj_oid, Oid roleid);
extern bool pg_package_ownercheck(Oid package_oid, Oid roleid);
extern bool pg_namespace_ownercheck(Oid nsp_oid, Oid roleid);
extern bool pg_tablespace_ownercheck(Oid spc_oid, Oid roleid);
extern bool pg_opclass_ownercheck(Oid opc_oid, Oid roleid);
extern bool pg_opfamily_ownercheck(Oid opf_oid, Oid roleid);
extern bool pg_database_ownercheck(Oid db_oid, Oid roleid);
extern bool pg_directory_ownercheck(Oid dir_oid, Oid roleid);
extern bool pg_collation_ownercheck(Oid coll_oid, Oid roleid);
extern bool pg_conversion_ownercheck(Oid conv_oid, Oid roleid);
extern bool pg_ts_dict_ownercheck(Oid dict_oid, Oid roleid);
extern bool pg_ts_config_ownercheck(Oid cfg_oid, Oid roleid);
extern bool pg_foreign_data_wrapper_ownercheck(Oid srv_oid, Oid roleid);
extern bool pg_foreign_server_ownercheck(Oid srv_oid, Oid roleid);
extern bool pg_extension_ownercheck(Oid ext_oid, Oid roleid);
extern bool gs_sec_cmk_ownercheck(Oid key_oid, Oid roleid);
extern bool gs_sec_cek_ownercheck(Oid key_oid, Oid roleid);
extern bool pg_extension_data_source_ownercheck(Oid src_oid, Oid roleid);
extern bool pg_synonym_ownercheck(Oid synOid, Oid roleId);
extern bool has_createrole_privilege(Oid roleid);
extern bool has_auditadmin_privilege(Oid roleid);
extern Acl* getAclNodeGroup();
extern void grantNodeGroupToRole(Oid group_id, Oid roleid, AclMode privileges, bool is_grant);
extern void check_nodegroup_privilege(Oid roleid, Oid ownerId, AclMode mode);
extern Oid get_nodegroup_member_of(Oid roleid);
extern Oid get_nodegroup_privs_of(Oid roleid);
extern bool is_role_independent(Oid roleid);
extern bool is_role_iamauth(Oid roleid);
extern bool independent_priv_aclcheck(AclMode mask, char relkind);
extern bool is_trust_language(Oid lang_oid);
extern Acl* allocacl(int n);
extern bool pg_publication_ownercheck(Oid pub_oid, Oid roleid);
extern bool pg_subscription_ownercheck(Oid sub_oid, Oid roleid);

#endif /* ACL_H */
