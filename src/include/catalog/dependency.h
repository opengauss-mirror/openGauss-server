/* -------------------------------------------------------------------------
 *
 * dependency.h
 *	  Routines to support inter-object dependencies.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/dependency.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DEPENDENCY_H
#define DEPENDENCY_H

#include "catalog/objectaddress.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/pg_directory.h"


/*
 * Precise semantics of a dependency relationship are specified by the
 * DependencyType code (which is stored in a "char" field in pg_depend,
 * so we assign ASCII-code values to the enumeration members).
 *
 * In all cases, a dependency relationship indicates that the referenced
 * object may not be dropped without also dropping the dependent object.
 * However, there are several subflavors:
 *
 * DEPENDENCY_NORMAL ('n'): normal relationship between separately-created
 * objects.  The dependent object may be dropped without affecting the
 * referenced object.  The referenced object may only be dropped by
 * specifying CASCADE, in which case the dependent object is dropped too.
 * Example: a table column has a normal dependency on its datatype.
 *
 * DEPENDENCY_AUTO ('a'): the dependent object can be dropped separately
 * from the referenced object, and should be automatically dropped
 * (regardless of RESTRICT or CASCADE mode) if the referenced object
 * is dropped.
 * Example: a named constraint on a table is made auto-dependent on
 * the table, so that it will go away if the table is dropped.
 *
 * DEPENDENCY_INTERNAL ('i'): the dependent object was created as part
 * of creation of the referenced object, and is really just a part of
 * its internal implementation.  A DROP of the dependent object will be
 * disallowed outright (we'll tell the user to issue a DROP against the
 * referenced object, instead).  A DROP of the referenced object will be
 * propagated through to drop the dependent object whether CASCADE is
 * specified or not.
 * Example: a trigger that's created to enforce a foreign-key constraint
 * is made internally dependent on the constraint's pg_constraint entry.
 *
 * DEPENDENCY_EXTENSION ('e'): the dependent object is a member of the
 * extension that is the referenced object.  The dependent object can be
 * dropped only via DROP EXTENSION on the referenced object.  Functionally
 * this dependency type acts the same as an internal dependency, but it's
 * kept separate for clarity and to simplify pg_dump.
 *
 * DEPENDENCY_PIN ('p'): there is no dependent object; this type of entry
 * is a signal that the system itself depends on the referenced object,
 * and so that object must never be deleted.  Entries of this type are
 * created only during initdb.	The fields for the dependent object
 * contain zeroes.
 *
 * Other dependency flavors may be needed in future.
 */

typedef enum DependencyType {
	DEPENDENCY_NORMAL = 'n',
	DEPENDENCY_AUTO = 'a',
	DEPENDENCY_INTERNAL = 'i',
	DEPENDENCY_EXTENSION = 'e',
	DEPENDENCY_PIN = 'p'
} DependencyType;

/*
 * Deletion processing requires additional state for each ObjectAddress that
 * it's planning to delete.  For simplicity and code-sharing we make the
 * ObjectAddresses code support arrays with or without this extra state.
 */
typedef struct {
	int			flags;   /* bitmask, see bit definitions below */
	ObjectAddress dependee;	         /* object whose deletion forced this one */
} ObjectAddressExtra;

/* expansible list of ObjectAddresses */
struct ObjectAddresses {
	ObjectAddress *refs;             /* => palloc'd array */
	ObjectAddressExtra *extras;      /* => palloc'd array, or NULL if not used */
	int			numrefs; /* current number of references */
	int			maxrefs; /* current size of palloc'd array(s) */
};

/*
 * There is also a SharedDependencyType enum type that determines the exact
 * semantics of an entry in pg_shdepend.  Just like regular dependency entries,
 * any pg_shdepend entry means that the referenced object cannot be dropped
 * unless the dependent object is dropped at the same time.  There are some
 * additional rules however:
 *
 * (a) For a SHARED_DEPENDENCY_PIN entry, there is no dependent object --
 * rather, the referenced object is an essential part of the system.  This
 * applies to the initdb-created superuser.  Entries of this type are only
 * created by initdb; objects in this category don't need further pg_shdepend
 * entries if more objects come to depend on them.
 *
 * (b) a SHARED_DEPENDENCY_OWNER entry means that the referenced object is
 * the role owning the dependent object.  The referenced object must be
 * a pg_authid entry.
 *
 * (c) a SHARED_DEPENDENCY_ACL entry means that the referenced object is
 * a role mentioned in the ACL field of the dependent object.  The referenced
 * object must be a pg_authid entry.  (SHARED_DEPENDENCY_ACL entries are not
 * created for the owner of an object; hence two objects may be linked by
 * one or the other, but not both, of these dependency types.)
 *
 * (d) a SHARED_DEPENDENCY_RLSPOLICY entry means that the referenced object
 * is the role mentioned in a policy object. The referenced object must be a
 * pg_authid entry.
 *
 * (e) a SHARED_DEPENDENCY_MOT_TABLE entry means that the referenced object
 * is the database holding FDW table. The dependent object must be a FDW table entry.
 *
 * (f) a SHARED_DEPENDENCY_DBPRIV entry means that the referenced object is
 * a role mentioned in the gs_db_privilege.  The referenced object must be a pg_authid entry.
 *
 * SHARED_DEPENDENCY_INVALID is a value used as a parameter in internal
 * routines, and is not valid in the catalog itself.
 */
typedef enum SharedDependencyType {
	SHARED_DEPENDENCY_PIN = 'p',
	SHARED_DEPENDENCY_OWNER = 'o',
	SHARED_DEPENDENCY_ACL = 'a',
	SHARED_DEPENDENCY_RLSPOLICY = 'r',
	SHARED_DEPENDENCY_MOT_TABLE = 'm',
	SHARED_DEPENDENCY_DBPRIV = 'd',
	SHARED_DEPENDENCY_INVALID = 0
} SharedDependencyType;

/* expansible list of ObjectAddresses (private in dependency.c) */
typedef struct ObjectAddresses ObjectAddresses;

/* threaded list of ObjectAddresses, for recursion detection */
typedef struct ObjectAddressStack {
    const ObjectAddress* object;     /* object being visited */
    int flags;                       /* its current flag bits */
    struct ObjectAddressStack* next; /* next outer stack level */
} ObjectAddressStack;

/* for find_expr_references_walker */
typedef struct {
    ObjectAddresses* addrs; /* addresses being accumulated */
    List* rtables;          /* list of rangetables to resolve Vars */
} find_expr_references_context;

/*
 * This enum covers all system catalogs whose OIDs can appear in
 * pg_depend.classId or pg_shdepend.classId.
 */
typedef enum ObjectClass {
	OCLASS_CLASS,            /* pg_class */
	OCLASS_PROC,             /* pg_proc */
	OCLASS_TYPE,             /* pg_type */
	OCLASS_CAST,             /* pg_cast */
	OCLASS_COLLATION,        /* pg_collation */
	OCLASS_CONSTRAINT,       /* pg_constraint */
	OCLASS_CONVERSION,       /* pg_conversion */
	OCLASS_DEFAULT,	         /* pg_attrdef */
	OCLASS_LANGUAGE,         /* pg_language */
	OCLASS_LARGEOBJECT,      /* pg_largeobject */
	OCLASS_OPERATOR,         /* pg_operator */
	OCLASS_OPCLASS,	         /* pg_opclass */
	OCLASS_OPFAMILY,         /* pg_opfamily */
	OCLASS_AMOP,             /* pg_amop */
	OCLASS_AMPROC,           /* pg_amproc */
	OCLASS_REWRITE,          /* pg_rewrite */
	OCLASS_TRIGGER,          /* pg_trigger */
	OCLASS_SCHEMA,           /* pg_namespace */
	OCLASS_TSPARSER,         /* pg_ts_parser */
	OCLASS_TSDICT,	         /* pg_ts_dict */
	OCLASS_TSTEMPLATE,       /* pg_ts_template */
	OCLASS_TSCONFIG,         /* pg_ts_config */
	OCLASS_ROLE,             /* pg_authid */
	OCLASS_DATABASE,         /* pg_database */
	OCLASS_TBLSPACE,         /* pg_tablespace */
	OCLASS_FDW,              /* pg_foreign_data_wrapper */
	OCLASS_FOREIGN_SERVER,   /* pg_foreign_server */
	OCLASS_USER_MAPPING,     /* pg_user_mapping */
	OCLASS_SYNONYM,	         /* pg_synonym */
#ifdef PGXC
	OCLASS_PGXC_CLASS,       /* pgxc_class */
	OCLASS_PGXC_NODE,        /* pgxc_node */
	OCLASS_PGXC_GROUP,       /* pgxc_group */
#endif
	OCLASS_GLOBAL_SETTING,                  /* global setting */
    OCLASS_COLUMN_SETTING,                  /* column setting */
    OCLASS_CL_CACHED_COLUMN,        /* client logic cached column */
    OCLASS_GLOBAL_SETTING_ARGS,        /* global setting args */
    OCLASS_COLUMN_SETTING_ARGS,        /* column setting args */
	OCLASS_DEFACL,           /* pg_default_acl */
    OCLASS_DB_PRIVILEGE,     /* gs_db_privilege */
	OCLASS_EXTENSION,        /* pg_extension */
	OCLASS_DATA_SOURCE,      /* data source */
	OCLASS_DIRECTORY,        /* pg_directory */
	OCLASS_PG_JOB,           /* pg_job */
	OCLASS_RLSPOLICY,        /* pg_rlspolicy */
	OCLASS_DB4AI_MODEL,      /* gs_model_warehouse */
    OCLASS_GS_CL_PROC,       /* client logic procedures */
    OCLASS_PACKAGE,          /* gs_package */
    OCLASS_PUBLICATION,      /* pg_publication */
    OCLASS_PUBLICATION_REL,  /* pg_publication_rel */
    OCLASS_SUBSCRIPTION,     /* pg_subscription */
	MAX_OCLASS               /* MUST BE LAST */
} ObjectClass;


/* in dependency.c */
#define PERFORM_DELETION_INVALID            0x0000
#define PERFORM_DELETION_INTERNAL			0x0001
#define PERFORM_DELETION_CONCURRENTLY		0x0002

/* ObjectAddressExtra flag bits */
#define DEPFLAG_ORIGINAL 0x0001  /* an original deletion target */
#define DEPFLAG_NORMAL 0x0002    /* reached via normal dependency */
#define DEPFLAG_AUTO 0x0004      /* reached via auto dependency */
#define DEPFLAG_INTERNAL 0x0008  /* reached via internal dependency */
#define DEPFLAG_EXTENSION 0x0010 /* reached via extension dependency */
#define DEPFLAG_REVERSE 0x0020   /* reverse internal/extension link */


extern void performDeletion(const ObjectAddress *object,
                            DropBehavior behavior,
                            int flags);

extern void performMultipleDeletions(const ObjectAddresses *objects,
                                     DropBehavior behavior,
                                     uint32 flags,
                                     bool isPkgDropTypes = false);

extern void deleteWhatDependsOn(const ObjectAddress *object,
                                bool showNotices);

extern void recordDependencyOnExpr(const ObjectAddress *depender,
                                   Node *expr,
                                   List *rtable,
                                   DependencyType behavior);

extern void recordDependencyOnSingleRelExpr(const ObjectAddress *depender,
                                            Node *expr,
                                            Oid relId,
                                            DependencyType behavior,
                                            DependencyType self_behavior);

extern ObjectClass getObjectClass(const ObjectAddress *object);

extern char *getObjectDescription(const ObjectAddress *object);
extern char *getObjectDescriptionOids(Oid classid, Oid objid);

extern ObjectAddresses *new_object_addresses(const int maxRefs = 32);

extern void add_exact_object_address(const ObjectAddress *object,
                                     ObjectAddresses *addrs);

extern bool object_address_present(const ObjectAddress *object,
                                   const ObjectAddresses *addrs);

extern void record_object_address_dependencies(const ObjectAddress *depender,
                                               ObjectAddresses *referenced,
                                               DependencyType behavior);

extern void free_object_addresses(ObjectAddresses *addrs);

/* in pg_depend.c */
extern void recordDependencyOn(const ObjectAddress *depender,
                               const ObjectAddress *referenced,
                               DependencyType behavior);

extern void recordMultipleDependencies(const ObjectAddress *depender,
                                       const ObjectAddress *referenced,
                                       int nreferenced,
                                       DependencyType behavior);

extern void recordDependencyOnCurrentExtension(const ObjectAddress *object,
                                               bool isReplace);

extern void recordPinnedDependency(const ObjectAddress *object);

extern bool IsPackageDependType(Oid typOid, Oid pkgOid, bool isRefCur = false);

extern long DeleteTypesDenpendOnPackage(Oid classId, Oid objectId, bool isSpec = true);

extern long deleteDependencyRecordsFor(Oid classId,
                                       Oid objectId,
                                       bool skipExtensionDeps);

extern long deleteDependencyRecordsForClass(Oid classId, 
                                            Oid objectId,
                                            Oid refclassId,
                                            char deptype);

extern long changeDependencyFor(Oid classId,
                                Oid objectId,
                                Oid refClassId, 
                                Oid oldRefObjectId,
                                Oid newRefObjectId);

extern Oid	getExtensionOfObject(Oid classId, Oid objectId);

extern bool sequenceIsOwned(Oid seqId, Oid *tableId, int32 *colId);

extern void markSequenceUnowned(Oid seqId);

extern List *getOwnedSequences(Oid relid, List *attrList = NULL);

extern Oid	get_constraint_index(Oid constraintId);

extern Oid	get_index_constraint(Oid indexId);

/* in pg_shdepend.c */
extern void recordSharedDependencyOn(ObjectAddress *depender,
                                     ObjectAddress *referenced,
                                     SharedDependencyType deptype,
                                     const char *objfile = NULL);

extern void deleteSharedDependencyRecordsFor(Oid classId,
                                             Oid objectId,
                                             int32 objectSubId);

extern void recordDependencyOnOwner(Oid classId, Oid objectId, Oid owner, const char *objfile = NULL);
extern void recordDependencyOnPackage(Oid classId, Oid objectId, List* pkgOidList);
#ifdef ENABLE_MOT
extern void recordDependencyOnDatabase(Oid classId, Oid objectId, Oid serverId, Oid owner, const char *objfile = NULL);
#endif

extern void changeDependencyOnOwner(Oid classId, 
                                    Oid objectId,
                                    Oid newOwnerId);

extern void updateAclDependencies(Oid classId, 
                                  Oid objectId,
                                  int32 objectSubId,
                                  Oid ownerId,
                                  int noldmembers,
                                  Oid *oldmembers,
                                  int nnewmembers, 
                                  Oid *newmembers);

extern bool checkSharedDependencies(Oid classId, 
                                    Oid objectId,
                                    char **detail_msg,
                                    char **detail_log_msg);

extern void shdepLockAndCheckObject(Oid classId, Oid objectId);

extern void copyTemplateDependencies(Oid templateDbId, Oid newDbId);

extern void dropDatabaseDependencies(Oid databaseId);

extern void shdepDropOwned(List *relids, DropBehavior behavior);

extern void shdepReassignOwned(List *relids, Oid newrole);

extern bool TSConfigurationHasDependentObjects(Oid tsconfoid);

extern void recordDependencyOnRespool(Oid classId, Oid objectId, Oid owner);

extern bool CheckDependencyOnRespool(Oid classId, Oid objectId, List **foundlist, bool full);
extern void changeDependencyOnRespool(Oid classId, Oid objectId, Oid newOwnerId);
extern void prepareDatabaseCFunLibrary(Oid databaseId);

extern void deleteDictionaryTSFile(Oid dictId);
extern void deleteDatabaseTSFile(Oid databaseId);
extern void changeDependencyOnObjfile(Oid objectId, Oid refobjId, const char *newObjfile);

#ifdef ENABLE_MULTIPLE_NODES
namespace Tsdb {
extern void performTsCudescDeletion(List* cudesc_oids);
}
#endif   /* ENABLE_MULTIPLE_NODES */

extern void findDependentObjects(const ObjectAddress* object, int flags, ObjectAddressStack* stack,
    ObjectAddresses* targetObjects, const ObjectAddresses* pendingObjects, Relation* depRel);
extern void reportDependentObjects(
    const ObjectAddresses* targetObjects, DropBehavior behavior, int msglevel, const ObjectAddress* origObject);
extern void AcquireDeletionLock(const ObjectAddress* object, int flags);
extern void add_object_address_ext(Oid classId, Oid objectId, int32 subId, char deptype, ObjectAddresses* addrs);
extern void add_object_address_ext1(const ObjectAddress *obj, ObjectAddresses* addrs);

#endif   /* DEPENDENCY_H */
