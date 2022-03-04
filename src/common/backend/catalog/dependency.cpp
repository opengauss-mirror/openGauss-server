/* -------------------------------------------------------------------------
 *
 * dependency.cpp
 *	  Routines to support inter-object dependencies.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/dependency.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/dfs/dfs_insert.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/gs_matview.h"
#include "catalog/gs_model.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_fn.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_directory.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_job.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/gs_package.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/storage.h"
#include "catalog/pg_extension_data_source.h"
#ifdef PGXC
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_slice.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/directory.h"
#include "gtm/gtm_c.h"
#include "access/gtm.h"
#endif
#include "client_logic/client_logic.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/matview.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteRemove.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "datasource/datasource.h"
#include "postmaster/rbcleaner.h"

/*
 * This constant table maps ObjectClasses to the corresponding catalog OIDs.
 * See also getObjectClass().
 */
static const Oid object_classes[MAX_OCLASS] = {
    RelationRelationId,              /* OCLASS_CLASS */
    ProcedureRelationId,             /* OCLASS_PROC */
    TypeRelationId,                  /* OCLASS_TYPE */
    CastRelationId,                  /* OCLASS_CAST */
    CollationRelationId,             /* OCLASS_COLLATION */
    ConstraintRelationId,            /* OCLASS_CONSTRAINT */
    ConversionRelationId,            /* OCLASS_CONVERSION */
    AttrDefaultRelationId,           /* OCLASS_DEFAULT */
    LanguageRelationId,              /* OCLASS_LANGUAGE */
    LargeObjectRelationId,           /* OCLASS_LARGEOBJECT */
    OperatorRelationId,              /* OCLASS_OPERATOR */
    OperatorClassRelationId,         /* OCLASS_OPCLASS */
    OperatorFamilyRelationId,        /* OCLASS_OPFAMILY */
    AccessMethodOperatorRelationId,  /* OCLASS_AMOP */
    AccessMethodProcedureRelationId, /* OCLASS_AMPROC */
    RewriteRelationId,               /* OCLASS_REWRITE */
    TriggerRelationId,               /* OCLASS_TRIGGER */
    NamespaceRelationId,             /* OCLASS_SCHEMA */
    TSParserRelationId,              /* OCLASS_TSPARSER */
    TSDictionaryRelationId,          /* OCLASS_TSDICT */
    TSTemplateRelationId,            /* OCLASS_TSTEMPLATE */
    TSConfigRelationId,              /* OCLASS_TSCONFIG */
    AuthIdRelationId,                /* OCLASS_ROLE */
    DatabaseRelationId,              /* OCLASS_DATABASE */
    TableSpaceRelationId,            /* OCLASS_TBLSPACE */
    ForeignDataWrapperRelationId,    /* OCLASS_FDW */
    ForeignServerRelationId,         /* OCLASS_FOREIGN_SERVER */
    UserMappingRelationId,           /* OCLASS_USER_MAPPING */
    PgSynonymRelationId,             /* OCLASS_SYNONYM */
    DefaultAclRelationId,            /* OCLASS_DEFACL */
    DbPrivilegeId,                   /* OCLASS_DB_PRIVILEGE */
    ExtensionRelationId,             /* OCLASS_EXTENSION */
    PgDirectoryRelationId,           /* OCLASS_DIRECTORY */
    PgJobRelationId,                 /* OCLASS_PG_JOB */
    PublicationRelationId,           /* OCLASS_PUBLICATION */
    PublicationRelRelationId,        /* OCLASS_PUBLICATION_REL */
    SubscriptionRelationId           /* OCLASS_SUBSCRIPTION */
#ifdef PGXC
    ,
    PgxcClassRelationId /* OCLASS_PGXCCLASS */
#endif

};

#ifdef ENABLE_MULTIPLE_NODES
namespace Tsdb {
static void findTsDependentObjects(ObjectAddresses* targetObjects, Relation depRel);
}
#endif   /* ENABLE_MULTIPLE_NODES */
static void deleteOneObject(const ObjectAddress* object, Relation* depRel, int32 flags);
static void doDeletion(const ObjectAddress* object, int flags);
static void ReleaseDeletionLock(const ObjectAddress* object);
static bool find_expr_references_walker(Node* node, find_expr_references_context* context);
static void eliminate_duplicate_dependencies(ObjectAddresses* addrs);
static int object_address_comparator(const void* a, const void* b);
static void add_object_address(ObjectClass oclass, Oid objectId, int32 subId, ObjectAddresses* addrs);
static void add_exact_object_address_extra(
    const ObjectAddress* object, const ObjectAddressExtra* extra, ObjectAddresses* addrs);
static bool object_address_present_add_flags(const ObjectAddress* object, int flags, ObjectAddresses* addrs);
static bool stack_address_present_add_flags(const ObjectAddress* object, int flags, ObjectAddressStack* stack);
static void getRelationDescription(StringInfo buffer, Oid relid);
static void getOpFamilyDescription(StringInfo buffer, Oid opfid);
static void PreCheckForDfsTable(ObjectAddresses* targetObjects);
extern char* pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn);
extern char* pg_get_functiondef_worker(Oid funcid, int* headerlines);

/*
 * performDeletion: attempt to drop the specified object.  If CASCADE
 * behavior is specified, also drop any dependent objects (recursively).
 * If RESTRICT behavior is specified, error out if there are any dependent
 * objects, except for those that should be implicitly dropped anyway
 * according to the dependency type.
 *
 * This is the outer control routine for all forms of DROP that drop objects
 * that can participate in dependencies.  Note that the next two routines
 * are variants on the same theme; if you change anything here you'll likely
 * need to fix them too.
 *
 * flags should include PERFORM_DELETION_INTERNAL when the drop operation is
 * not the direct result of a user-initiated action.  For example, when a
 * temporary schema is cleaned out so that a new backend can use it, or when
 * a column default is dropped as an intermediate step while adding a new one,
 * that's an internal operation.  On the other hand, when the we drop something
 * because the user issued a DROP statement against it, that's not internal.
 */
void performDeletion(const ObjectAddress* object, DropBehavior behavior, int flags)
{
    Relation depRel;
    ObjectAddresses* targetObjects = NULL;
    int i;

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = heap_open(DependRelationId, RowExclusiveLock);

    /*
     * Acquire deletion lock on the target object.	(Ideally the caller has
     * done this already, but many places are sloppy about it.)
     */
    AcquireDeletionLock(object, 0);

    /*
     * Construct a list of objects to delete (ie, the given object plus
     * everything directly or indirectly dependent on it).
     */
    targetObjects = new_object_addresses();

    findDependentObjects(object,
        DEPFLAG_ORIGINAL,
        NULL, /* empty stack */
        targetObjects,
        NULL, /* no pendingObjects */
        &depRel);

    bool has_encrypted_column = false;
    has_encrypted_column = is_exist_encrypted_column(targetObjects);
    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     */
    reportDependentObjects(targetObjects, behavior, NOTICE, object);

    if (!has_encrypted_column || behavior != DROP_CASCADE) {
        /*
        * Delete all the objects in the proper order.
        */
        for (i = 0; i < targetObjects->numrefs; i++) {
            ObjectAddress *thisobj = targetObjects->refs + i;

            deleteOneObject(thisobj, &depRel, flags);
        }
    }

    /* And clean up */
    free_object_addresses(targetObjects);

    heap_close(depRel, RowExclusiveLock);
}

/*
 * Don't allow "drop DFS table" to run inside a transaction block.
 *
 * "DfsDDLIsTopLevelXact" is set in "case T_DropStmt" of
 * standard_ProcessUtility()
 *
 * exception: allow "DROP DFS TABLE" operation in transaction block
 * during redis a table.
 */
static void PreCheckForDfsTable(ObjectAddresses* targetObjects)
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && false == u_sess->attr.attr_sql.enable_cluster_resize) {
        for (int i = 0; i < targetObjects->numrefs; i++) {
            ObjectAddress* thisobj = targetObjects->refs + i;

            if (RelationRelationId == thisobj->classId) {
                char relKind = get_rel_relkind(thisobj->objectId);

                if (relKind != RELKIND_INDEX && 0 == thisobj->objectSubId) {
                    if (RelationIsPaxFormatByOid(thisobj->objectId))
                        PreventTransactionChain(u_sess->exec_cxt.DfsDDLIsTopLevelXact, "DROP DFS TABLE");
                }
            }
        }
    }
}

static void PreDropForDfsTable(ObjectAddresses* targetObjects)
{
    if (!(IS_PGXC_DATANODE))
        return;

    for (int i = 0; i < targetObjects->numrefs; i++) {
        ObjectAddress* thisobj = targetObjects->refs + i;

        if (RelationRelationId == thisobj->classId) {
            char relKind = get_rel_relkind(thisobj->objectId);

            if (relKind != RELKIND_INDEX && 0 == thisobj->objectSubId) {
                Oid relid = thisobj->objectId;
                Relation rel;

                if (!OidIsValid(relid))
                    continue;

                // open relation
                rel = try_relation_open(relid, AccessShareLock);
                if (NULL == rel)
                    continue;

                // check DFS table
                if (RelationIsPAXFormat(rel)) {
                    // get DFS file name,  DFS file size from DFSDESC
                    DFSDescHandler handler(MAX_LOADED_DFSDESC, rel->rd_att->natts, rel);
                    SaveDfsFilelist(rel, &handler);
                }

                relation_close(rel, AccessShareLock);
            }
        }
    }
}

/*
 * performMultipleDeletions: Similar to performDeletion, but act on multiple
 * objects at once.
 *
 * The main difference from issuing multiple performDeletion calls is that the
 * list of objects that would be implicitly dropped, for each object to be
 * dropped, is the union of the implicit-object list for all objects.  This
 * makes each check be more relaxed.
 */
void performMultipleDeletions(const ObjectAddresses* objects, DropBehavior behavior, uint32 flags, bool isPkgDropTypes)
{
    Relation depRel;
    ObjectAddresses* targetObjects = NULL;
    int i;

    /* No work if no objects... */
    if (objects->numrefs <= 0)
        return;

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = heap_open(DependRelationId, RowExclusiveLock);

    /*
     * Construct a list of objects to delete (ie, the given objects plus
     * everything directly or indirectly dependent on them).  Note that
     * because we pass the whole objects list as pendingObjects context, we
     * won't get a failure from trying to delete an object that is internally
     * dependent on another one in the list; we'll just skip that object and
     * delete it when we reach its owner.
     */
    targetObjects = new_object_addresses();

    for (i = 0; i < objects->numrefs; i++) {
        const ObjectAddress* thisobj = objects->refs + i;
        if (getObjectClass(thisobj) == OCLASS_SCHEMA) {
            AcquireDeletionLock(thisobj, flags);
            RbCltPurgeSchema(thisobj->objectId);
        }
    }

    for (i = 0; i < objects->numrefs; i++) {
        const ObjectAddress* thisobj = objects->refs + i;

        /*
         * Acquire deletion lock on each target object.  (Ideally the caller
         * has done this already, but many places are sloppy about it.)
         */
        AcquireDeletionLock(thisobj, flags);

        /*
         * Purge all the rb objects in the proper order.
         */

        /*
         * Find dependent objects recursively.
         */
        findDependentObjects(thisobj,
            DEPFLAG_ORIGINAL,
            NULL, /* empty stack */
            targetObjects,
            objects,
            &depRel);

        ereport(LOG, (errmsg("Delete object %u/%u/%d", thisobj->classId, thisobj->objectId, thisobj->objectSubId)));
    }

    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     *
     * If there's exactly one object being deleted, report it the same way as
     * in performDeletion(), else we have to be vaguer.
     */
    if (isPkgDropTypes) {
        reportDependentObjects(targetObjects, behavior, INFO, ((objects->numrefs == 1) ? objects->refs : NULL));
    } else {
        reportDependentObjects(targetObjects, behavior, NOTICE, ((objects->numrefs == 1) ? objects->refs : NULL));
    }

    /*
     * Don't allow "drop DFS table" to run inside a transaction block.
     *
     * "DfsDDLIsTopLevelXact" is set in "case T_DropStmt" of
     * standard_ProcessUtility()
     */
    PreCheckForDfsTable(targetObjects);

    /*
     * write down the delete file name and size for DFS table
     * must called after the all delete objects are locked.
     */
    PreDropForDfsTable(targetObjects);

    MemoryContext oldCxt = CurrentMemoryContext;
    MemoryContext objDelCxt = AllocSetContextCreate(CurrentMemoryContext, "ObjectDeleteContext", ALLOCSET_SMALL_MINSIZE,
                                                    ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
    /*
     * Delete all the objects in the proper order.
     */
    for (i = 0; i < targetObjects->numrefs; i++) {
        (void)MemoryContextSwitchTo(objDelCxt);
        ObjectAddress* thisobj = targetObjects->refs + i;
        deleteOneObject(thisobj, &depRel, flags);
        MemoryContextReset(objDelCxt);
    }

    (void)MemoryContextSwitchTo(oldCxt);
    MemoryContextDelete(objDelCxt);

    /* And clean up */
    free_object_addresses(targetObjects);

    heap_close(depRel, RowExclusiveLock);
}

/*
 * deleteWhatDependsOn: attempt to drop everything that depends on the
 * specified object, though not the object itself.	Behavior is always
 * CASCADE.
 *
 * This is currently used only to clean out the contents of a schema
 * (namespace): the passed object is a namespace.  We normally want this
 * to be done silently, so there's an option to suppress NOTICE messages.
 */
void deleteWhatDependsOn(const ObjectAddress* object, bool showNotices)
{
    Relation depRel;
    ObjectAddresses* targetObjects = NULL;
    int i;

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = heap_open(DependRelationId, RowExclusiveLock);

    /*
     * Acquire deletion lock on the target object.	(Ideally the caller has
     * done this already, but many places are sloppy about it.)
     */
    AcquireDeletionLock(object, 0);

    /*
     * Construct a list of objects to delete (ie, the given object plus
     * everything directly or indirectly dependent on it).
     */
    targetObjects = new_object_addresses();

    findDependentObjects(object, DEPFLAG_ORIGINAL, NULL, /* empty stack */
        targetObjects, NULL, /* no pendingObjects */ 
        &depRel);

    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     */
    reportDependentObjects(targetObjects, DROP_CASCADE, showNotices ? NOTICE : DEBUG2, object);

    /*
     * Delete all the objects in the proper order, except we skip the original
     * object.
     */
    for (i = 0; i < targetObjects->numrefs; i++) {
        ObjectAddress* thisobj = targetObjects->refs + i;
        ObjectAddressExtra* thisextra = targetObjects->extras + i;

        if (thisextra->flags & DEPFLAG_ORIGINAL)
            continue;

        /*
         * Since this function is currently only used to clean out temporary
         * schemas, we pass PERFORM_DELETION_INTERNAL here, indicating that
         * the operation is an automatic system operation rather than a user
         * action.	If, in the future, this function is used for other
         * purposes, we might need to revisit this.
         */
        deleteOneObject(thisobj, &depRel, PERFORM_DELETION_INTERNAL);
    }

    /* And clean up */
    free_object_addresses(targetObjects);

    heap_close(depRel, RowExclusiveLock);
}

/*
 * findDependentObjects - find all objects that depend on 'object'
 *
 * For every object that depends on the starting object, acquire a deletion
 * lock on the object, add it to targetObjects (if not already there),
 * and recursively find objects that depend on it.	An object's dependencies
 * will be placed into targetObjects before the object itself; this means
 * that the finished list's order represents a safe deletion order.
 *
 * The caller must already have a deletion lock on 'object' itself,
 * but must not have added it to targetObjects.  (Note: there are corner
 * cases where we won't add the object either, and will also release the
 * caller-taken lock.  This is a bit ugly, but the API is set up this way
 * to allow easy rechecking of an object's liveness after we lock it.  See
 * notes within the function.)
 *
 * When dropping a whole object (subId = 0), we find dependencies for
 * its sub-objects too.
 *
 *	object: the object to add to targetObjects and find dependencies on
 *	flags: flags to be ORed into the object's targetObjects entry
 *	stack: list of objects being visited in current recursion; topmost item
 *			is the object that we recursed from (NULL for external callers)
 *	targetObjects: list of objects that are scheduled to be deleted
 *	pendingObjects: list of other objects slated for destruction, but
 *			not necessarily in targetObjects yet (can be NULL if none)
 *	*depRel: already opened pg_depend relation
 */
void findDependentObjects(const ObjectAddress* object, int flags, ObjectAddressStack* stack,
    ObjectAddresses* targetObjects, const ObjectAddresses* pendingObjects, Relation* depRel)
{
    ScanKeyData key[3];
    int nkeys;
    SysScanDesc scan;
    HeapTuple tup;
    ObjectAddress otherObject;
    ObjectAddressStack mystack;
    ObjectAddressExtra extra;

    /*
     * If the target object is already being visited in an outer recursion
     * level, just report the current flags back to that level and exit. This
     * is needed to avoid infinite recursion in the face of circular
     * dependencies.
     *
     * The stack check alone would result in dependency loops being broken at
     * an arbitrary point, ie, the first member object of the loop to be
     * visited is the last one to be deleted.  This is obviously unworkable.
     * However, the check for internal dependency below guarantees that we
     * will not break a loop at an internal dependency: if we enter the loop
     * at an "owned" object we will switch and start at the "owning" object
     * instead.  We could probably hack something up to avoid breaking at an
     * auto dependency, too, if we had to.	However there are no known cases
     * where that would be necessary.
     */
    if (stack_address_present_add_flags(object, flags, stack))
        return;

    /*
     * It's also possible that the target object has already been completely
     * processed and put into targetObjects.  If so, again we just add the
     * specified flags to its entry and return.
     *
     * (Note: in these early-exit cases we could release the caller-taken
     * lock, since the object is presumably now locked multiple times; but it
     * seems not worth the cycles.)
     */
    if (object_address_present_add_flags(object, flags, targetObjects))
        return;

    /*
     * The target object might be internally dependent on some other object
     * (its "owner"), and/or be a member of an extension (also considered its
     * owner).	If so, and if we aren't recursing from the owning object, we
     * have to transform this deletion request into a deletion request of the
     * owning object.  (We'll eventually recurse back to this object, but the
     * owning object has to be visited first so it will be deleted after.) The
     * way to find out about this is to scan the pg_depend entries that show
     * what this object depends on.
     */
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    if (object->objectSubId != 0) {
        ScanKeyInit(
            &key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(object->objectSubId));
        nkeys = 3;
    } else
        nkeys = 2;

    scan = systable_beginscan(*depRel, DependDependerIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend)GETSTRUCT(tup);

        otherObject.classId = foundDep->refclassid;
        otherObject.objectId = foundDep->refobjid;
        otherObject.objectSubId = foundDep->refobjsubid;

        switch (foundDep->deptype) {
            case DEPENDENCY_NORMAL:
            case DEPENDENCY_AUTO:
                /* no problem */
                break;
            case DEPENDENCY_INTERNAL:
            case DEPENDENCY_EXTENSION:

                /*
                 * This object is part of the internal implementation of
                 * another object, or is part of the extension that is the
                 * other object.  We have three cases:
                 *
                 * 1. At the outermost recursion level, we normally disallow
                 * the DROP.  (We just ereport here, rather than proceeding,
                 * since no other dependencies are likely to be interesting.)
                 * However, there are exceptions.
                 */
                if (stack == NULL) {
                    char* otherObjDesc = NULL;

                    /*
                     * Exception 1a: if the owning object is listed in
                     * pendingObjects, just release the caller's lock and
                     * return.	We'll eventually complete the DROP when we
                     * reach that entry in the pending list.
                     */
                    if (pendingObjects && object_address_present(&otherObject, pendingObjects)) {
                        systable_endscan(scan);
                        /* need to release caller's lock; see notes below */
                        ReleaseDeletionLock(object);
                        return;
                    }

                    /*
                     * Exception 1b: if the owning object is the extension
                     * currently being created/altered, it's okay to continue
                     * with the deletion.  This allows dropping of an
                     * extension's objects within the extension's scripts, as
                     * well as corner cases such as dropping a transient
                     * object created within such a script.
                     */
                    if (creating_extension && otherObject.classId == ExtensionRelationId &&
                        otherObject.objectId == u_sess->cmd_cxt.CurrentExtensionObject)
                        break;

                    /* No exception applies, so throw the error */
                    otherObjDesc = getObjectDescription(&otherObject);
                    ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                            errmsg("cannot drop %s because %s requires it", getObjectDescription(object), otherObjDesc),
                            errhint("You can drop %s instead.", otherObjDesc)));
                }

                /*
                 * 2. When recursing from the other end of this dependency,
                 * it's okay to continue with the deletion.  This holds when
                 * recursing from a whole object that includes the nominal
                 * other end as a component, too.  Since there can be more
                 * than one "owning" object, we have to allow matches that are
                 * more than one level down in the stack.
                 */
                if (stack_address_present_add_flags(&otherObject, 0, stack))
                    break;

                /*
                 * 3. Not all the owning objects have been visited, so
                 * transform this deletion request into a delete of this
                 * owning object.
                 *
                 * First, release caller's lock on this object and get
                 * deletion lock on the owning object.	(We must release
                 * caller's lock to avoid deadlock against a concurrent
                 * deletion of the owning object.)
                 */
                ReleaseDeletionLock(object);
                AcquireDeletionLock(&otherObject, 0);

                /*
                 * The owning object might have been deleted while we waited
                 * to lock it; if so, neither it nor the current object are
                 * interesting anymore.  We test this by checking the
                 * pg_depend entry (see notes below).
                 */
                if (!systable_recheck_tuple(scan, tup)) {
                    systable_endscan(scan);
                    ReleaseDeletionLock(&otherObject);
                    return;
                }

                /*
                 * Okay, recurse to the owning object instead of proceeding.
                 *
                 * We do not need to stack the current object; we want the
                 * traversal order to be as if the original reference had
                 * linked to the owning object instead of this one.
                 *
                 * The dependency type is a "reverse" dependency: we need to
                 * delete the owning object if this one is to be deleted, but
                 * this linkage is never a reason for an automatic deletion.
                 */
                findDependentObjects(&otherObject, DEPFLAG_REVERSE, stack, targetObjects, pendingObjects, depRel);
                /* And we're done here. */
                systable_endscan(scan);
                return;
            case DEPENDENCY_PIN:

                /*
                 * Should not happen; PIN dependencies should have zeroes in
                 * the depender fields...
                 */
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("incorrect use of PIN dependency with %s", getObjectDescription(object))));
                break;
            default:
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized dependency type '%c' for %s", foundDep->deptype,
                                getObjectDescription(object))));
                break;
        }
    }

    systable_endscan(scan);

    /*
     * Now recurse to any dependent objects.  We must visit them first since
     * they have to be deleted before the current object.
     */
    mystack.object = object; /* set up a new stack level */
    mystack.flags = flags;
    mystack.next = stack;

    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    if (object->objectSubId != 0) {
        ScanKeyInit(
            &key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(object->objectSubId));
        nkeys = 3;
    } else
        nkeys = 2;

    scan = systable_beginscan(*depRel, DependReferenceIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend)GETSTRUCT(tup);
        int subflags;

        otherObject.classId = foundDep->classid;
        otherObject.objectId = foundDep->objid;
        otherObject.objectSubId = foundDep->objsubid;

        /*
         * Must lock the dependent object before recursing to it.
         */
        AcquireDeletionLock(&otherObject, 0);

        /*
         * The dependent object might have been deleted while we waited to
         * lock it; if so, we don't need to do anything more with it. We can
         * test this cheaply and independently of the object's type by seeing
         * if the pg_depend tuple we are looking at is still live. (If the
         * object got deleted, the tuple would have been deleted too.)
         */
        if (!systable_recheck_tuple(scan, tup)) {
            /* release the now-useless lock */
            ReleaseDeletionLock(&otherObject);
            /* and continue scanning for dependencies */
            continue;
        }

        /* Recurse, passing flags indicating the dependency type */
        switch (foundDep->deptype) {
            case DEPENDENCY_NORMAL:
                subflags = DEPFLAG_NORMAL;
                break;
            case DEPENDENCY_AUTO:
                subflags = DEPFLAG_AUTO;
                break;
            case DEPENDENCY_INTERNAL:
                subflags = DEPFLAG_INTERNAL;
                break;
            case DEPENDENCY_EXTENSION:
                subflags = DEPFLAG_EXTENSION;
                break;
            case DEPENDENCY_PIN:

                /*
                 * For a PIN dependency we just ereport immediately; there
                 * won't be any others to report.
                 *
                 * However, to enable manual rollback, we allow
                 * drop of pinned system catalogs during inplace upgrade.
                 */
                if (!u_sess->attr.attr_common.IsInplaceUpgrade) {
                    ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                            errmsg("cannot drop %s because it is required by the database system",
                                getObjectDescription(object))));
                    subflags = 0; /* keep compiler quiet */
                } else
                    continue;
                break;
            default:
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized dependency type '%c' for %s", foundDep->deptype,
                                getObjectDescription(object))));
                subflags = 0; /* keep compiler quiet */
                break;
        }

        findDependentObjects(&otherObject, subflags, &mystack, targetObjects, pendingObjects, depRel);
    }

    systable_endscan(scan);

    /*
     * Finally, we can add the target object to targetObjects.	Be careful to
     * include any flags that were passed back down to us from inner recursion
     * levels.
     */
    extra.flags = mystack.flags;
    if (stack != NULL)
        extra.dependee = *stack->object;
    else {
        errno_t rc = memset_s(&extra.dependee, sizeof(extra.dependee), 0, sizeof(extra.dependee));
        securec_check(rc, "", "");
    }
    add_exact_object_address_extra(object, &extra, targetObjects);
}

/*
 * reportDependentObjects - report about dependencies, and fail if RESTRICT
 *
 * Tell the user about dependent objects that we are going to delete
 * (or would need to delete, but are prevented by RESTRICT mode);
 * then error out if there are any and it's not CASCADE mode.
 *
 *	targetObjects: list of objects that are scheduled to be deleted
 *	behavior: RESTRICT or CASCADE
 *	msglevel: elog level for non-error report messages
 *	origObject: base object of deletion, or NULL if not available
 *		(the latter case occurs in DROP OWNED)
 */
void reportDependentObjects(
    const ObjectAddresses* targetObjects, DropBehavior behavior, int msglevel, const ObjectAddress* origObject)
{
    bool ok = true;
    StringInfoData clientdetail;
    StringInfoData logdetail;
    int numReportedClient = 0;
    int numNotReportedClient = 0;
    int i;

    /*
     * If no error is to be thrown, and the msglevel is too low to be shown to
     * either client or server log, there's no need to do any of the work.
     *
     * Note: this code doesn't know all there is to be known about elog
     * levels, but it works for NOTICE and DEBUG2, which are the only values
     * msglevel can currently have.  We also assume we are running in a normal
     * operating environment.
     */
    if (behavior == DROP_CASCADE && msglevel < client_min_messages &&
        (msglevel < log_min_messages || log_min_messages == LOG))
        return;

        /*
         * We limit the number of dependencies reported to the client to
         * MAX_REPORTED_DEPS, since client software may not deal well with
         * enormous error strings.	The server log always gets a full report.
         */
#define MAX_REPORTED_DEPS 100

    initStringInfo(&clientdetail);
    initStringInfo(&logdetail);

    /*
     * We process the list back to front (ie, in dependency order not deletion
     * order), since this makes for a more understandable display.
     */
    for (i = targetObjects->numrefs - 1; i >= 0; i--) {
        const ObjectAddress* obj = &targetObjects->refs[i];
        const ObjectAddressExtra* extra = &targetObjects->extras[i];
        char* objDesc = NULL;

        /* Ignore the original deletion target(s) */
        if (extra->flags & DEPFLAG_ORIGINAL)
            continue;

        objDesc = getObjectDescription(obj);

        /*
         * If, at any stage of the recursive search, we reached the object via
         * an AUTO, INTERNAL, or EXTENSION dependency, then it's okay to
         * delete it even in RESTRICT mode.
         */
        if (extra->flags & (DEPFLAG_AUTO | DEPFLAG_INTERNAL | DEPFLAG_EXTENSION)) {
            /*
             * auto-cascades are reported at DEBUG2, not msglevel.	We don't
             * try to combine them with the regular message because the
             * results are too confusing when client_min_messages and
             * log_min_messages are different.
             */
            ereport(DEBUG2, (errmsg("drop auto-cascades to %s", objDesc)));
        } else if (behavior == DROP_RESTRICT) {
            char* otherDesc = getObjectDescription(&extra->dependee);

            if (numReportedClient < MAX_REPORTED_DEPS) {
                /* separate entries with a newline */
                if (clientdetail.len != 0)
                    appendStringInfoChar(&clientdetail, '\n');
                appendStringInfo(&clientdetail, _("%s depends on %s"), objDesc, otherDesc);
                numReportedClient++;
            } else
                numNotReportedClient++;
            /* separate entries with a newline */
            if (logdetail.len != 0)
                appendStringInfoChar(&logdetail, '\n');
            appendStringInfo(&logdetail, _("%s depends on %s"), objDesc, otherDesc);
            pfree_ext(otherDesc);
            ok = false;
        } else {
            if (u_sess->attr.attr_common.IsInplaceUpgrade) {
                if (strstr(objDesc, "view") != NULL) {
                    char* viewdef = pg_get_viewdef_worker(obj->objectId, 0, -1);

                    /* separate entries with a newline */
                    if (clientdetail.len != 0)
                        appendStringInfoChar(&clientdetail, '\n');
                    appendStringInfo(&clientdetail, _("Sorry that we have to drop %s due to "
                          "system catalog upgrade. You may rebuild it with the following sql after upgrade:\n"
                          "CREATE %s AS %s"), objDesc, objDesc, viewdef);

                    /* separate entries with a newline */
                    if (logdetail.len != 0)
                        appendStringInfoChar(&logdetail, '\n');
                    appendStringInfo(&logdetail, _("Sorry that we have to drop %s due to "
                          "system catalog upgrade. You may rebuild it with the following sql after upgrade:\n"
                          "CREATE %s AS %s"), objDesc, objDesc, viewdef);

                    pfree_ext(viewdef);
                } else if (strstr(objDesc, "function") != NULL) {
                    int headerlines = 0;
                    char* funcdef = pg_get_functiondef_worker(obj->objectId, &headerlines);

                    /* separate entries with a newline */
                    if (clientdetail.len != 0)
                        appendStringInfoChar(&clientdetail, '\n');
                    appendStringInfo(&clientdetail, _("Sorry that we have to drop %s due to "
                          "system catalog upgrade. You may rebuild it with the following sql after upgrade:\n%s"),
                        objDesc, funcdef);

                    /* separate entries with a newline */
                    if (logdetail.len != 0)
                        appendStringInfoChar(&logdetail, '\n');
                    appendStringInfo(&logdetail, _("Sorry that we have to drop %s due to "
                          "system catalog upgrade. You may rebuild it with the following sql after upgrade:\n%s"),
                        objDesc, funcdef);

                    pfree_ext(funcdef);
                } else if (strstr(objDesc, "cast from") != NULL) {
                    /* separate entries with a newline */
                    if (clientdetail.len != 0)
                        appendStringInfoChar(&clientdetail, '\n');
                    appendStringInfo(
                        &clientdetail, _("Sorry that we have to drop %s due to system catalog upgrade.\n"), objDesc);

                    /* separate entries with a newline */
                    if (logdetail.len != 0)
                        appendStringInfoChar(&logdetail, '\n');
                    appendStringInfo(
                        &logdetail, _("Sorry that we have to drop %s due to system catalog upgrade.\n"), objDesc);
                } else
                    ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                            errmsg(
                                "cannot drop %s cascadely during upgrade because it may contain user data", objDesc)));
            }
            if (strstr(objDesc, "encrypted column") != NULL) {
                ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop %s cascadely because encrypted column depend on it.",
                    getObjectDescription(origObject)),
                    errhint("we have to drop %s, ... before drop %s cascadely.", objDesc,
                    getObjectDescription(origObject))));
            }

            if (numReportedClient < MAX_REPORTED_DEPS || u_sess->attr.attr_common.IsInplaceUpgrade) {
                /* separate entries with a newline */
                if (clientdetail.len != 0)
                    appendStringInfoChar(&clientdetail, '\n');
                appendStringInfo(&clientdetail, _("drop cascades to %s"), objDesc);
                numReportedClient++;
            } else
                numNotReportedClient++;
            /* separate entries with a newline */
            if (logdetail.len != 0)
                appendStringInfoChar(&logdetail, '\n');
            appendStringInfo(&logdetail, _("drop cascades to %s"), objDesc);
        }

        pfree_ext(objDesc);
    }

    if (numNotReportedClient > 0)
        appendStringInfo(&clientdetail,
            ngettext("\nand %d other object "
                     "(see server log for list)",
                "\nand %d other objects "
                "(see server log for list)",
                numNotReportedClient),
            numNotReportedClient);

    if (!ok) {
        if (origObject != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop %s because other objects depend on it", getObjectDescription(origObject)),
                    errdetail("%s", clientdetail.data), errdetail_log("%s", logdetail.data),
                    errhint("Use DROP ... CASCADE to drop the dependent objects too.")));
        else
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop desired object(s) because other objects depend on them"),
                    errdetail("%s", clientdetail.data), errdetail_log("%s", logdetail.data),
                    errhint("Use DROP ... CASCADE to drop the dependent objects too.")));
    } else if (numReportedClient > 1) {
        ereport(msglevel,
            /* translator: %d always has a value larger than 1 */
            (errmsg_plural("drop cascades to %d other object", "drop cascades to %d other objects",
                 numReportedClient + numNotReportedClient, numReportedClient + numNotReportedClient),
                errdetail("%s", clientdetail.data), errdetail_log("%s", logdetail.data)));
    } else if (numReportedClient == 1) {
        /* we just use the single item as-is */
        ereport(msglevel, (errmsg_internal("%s", clientdetail.data)));
    }

    pfree_ext(clientdetail.data);
    pfree_ext(logdetail.data);
}

/*
 * deleteOneObject: delete a single object for performDeletion.
 *
 * *depRel is the already-open pg_depend relation.
 */
static void deleteOneObject(const ObjectAddress* object, Relation* depRel, int flags)
{
    ScanKeyData key[3];
    int nkeys;
    SysScanDesc scan;
    HeapTuple tup;

    /* DROP hook of the objects being removed */
    if (object_access_hook) {
        ObjectAccessDrop drop_arg;

        drop_arg.dropflags = flags;
        InvokeObjectAccessHook(OAT_DROP, object->classId, object->objectId, object->objectSubId, &drop_arg);
    }

    /*
     * Close depRel if we are doing a drop concurrently.  The object deletion
     * subroutine will commit the current transaction, so we can't keep the
     * relation open across doDeletion().
     */
    if ((unsigned int)flags & PERFORM_DELETION_CONCURRENTLY)
        heap_close(*depRel, RowExclusiveLock);

    /*
     * Delete the object itself, in an object-type-dependent way.
     *
     * We used to do this after removing the outgoing dependency links, but it
     * seems just as reasonable to do it beforehand.  In the concurrent case
     * we *must* do it in this order, because we can't make any transactional
     * updates before calling doDeletion() --- they'd get committed right
     * away, which is not cool if the deletion then fails.
     */
    doDeletion(object, flags);

    /*
     * Reopen depRel if we closed it above
     */
    if ((uint32)flags & PERFORM_DELETION_CONCURRENTLY)
        *depRel = heap_open(DependRelationId, RowExclusiveLock);

    /*
     * Now remove any pg_depend records that link from this object to others.
     * (Any records linking to this object should be gone already.)
     *
     * When dropping a whole object (subId = 0), remove all pg_depend records
     * for its sub-objects too.
     */
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    if (object->objectSubId != 0) {
        ScanKeyInit(
            &key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(object->objectSubId));
        nkeys = 3;
    } else
        nkeys = 2;

    scan = systable_beginscan(*depRel, DependDependerIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        simple_heap_delete(*depRel, &tup->t_self);
    }

    systable_endscan(scan);

    /*
     * During inplace upgrade, we may need to drop newly created system
     * catalogs even if it is pinned, in order to do manual rollback.
     */
    if (u_sess->attr.attr_common.IsInplaceUpgrade) {
        ScanKeyInit(
            &key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));
        ScanKeyInit(
            &key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
        if (object->objectSubId != 0) {
            ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(object->objectSubId));
            nkeys = 3;
        } else
            nkeys = 2;

        scan = systable_beginscan(*depRel, DependReferenceIndexId, true, NULL, nkeys, key);

        while (HeapTupleIsValid(tup = systable_getnext(scan))) {
            Form_pg_depend Pinnedobj = (Form_pg_depend)GETSTRUCT(tup);

            if (Pinnedobj->deptype == DEPENDENCY_PIN)
                simple_heap_delete(*depRel, &tup->t_self);
        }

        systable_endscan(scan);
    }

    /*
     * Delete shared dependency references related to this object.	Again, if
     * subId = 0, remove records for sub-objects too.
     */
    deleteSharedDependencyRecordsFor(object->classId, object->objectId, object->objectSubId);

    /*
     * Delete any comments or security labels associated with this object.
     * (This is a convenient place to do these things, rather than having
     * every object type know to do it.)
     */
    DeleteComments(object->objectId, object->classId, object->objectSubId);
    DeleteSecurityLabel(object);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();

    /*
     * And we're done!
     */
}

/*
 * doDeletion: actually delete a single object
 */
static void doDeletion(const ObjectAddress* object, int flags)
{
    switch (getObjectClass(object)) {
        case OCLASS_CLASS: {
            char relKind = get_rel_relkind(object->objectId);
            Relation relseq = NULL;
            char* seqname = NULL;
            bool isTmpSequence = false;
            bool isTmpTable = false;

            if (relKind == RELKIND_INDEX || relKind == RELKIND_GLOBAL_INDEX) {
                bool concurrent = (((uint32)flags & PERFORM_DELETION_CONCURRENTLY) == PERFORM_DELETION_CONCURRENTLY);

                Assert(object->objectSubId == 0);
                index_drop(object->objectId, concurrent);
            } else {
                /*
                 * relation_open() must be before the heap_drop_with_catalog(). If you reload
                 * relation after drop, it may cause other exceptions during the drop process.
                 */
                if (relKind == RELKIND_RELATION)
                    isTmpTable = IsTempTable(object->objectId);
                else if (RELKIND_IS_SEQUENCE(relKind)) {
                    isTmpSequence = IsTempSequence(object->objectId);
                    /*
                     * A relation is opened to get the schema and database name as
                     * such data is not available before when dropping a function.
                     */
                    relseq = relation_open(object->objectId, AccessShareLock);
                    seqname = GetGlobalSeqName(relseq, NULL, NULL);

                    /* Then close the relation opened previously */
                    relation_close(relseq, AccessShareLock);
                } else if (relKind == RELKIND_MATVIEW) {
                    delete_matview_tuple(object->objectId);
                    delete_matviewdep_tuple(object->objectId);
                }

                if (object->objectSubId != 0)
                    RemoveAttributeById(object->objectId, object->objectSubId);
                else
                    heap_drop_with_catalog(object->objectId);

                /*
                 * IMPORANT: The relation must not be reloaded after heap_drop_with_catalog()
                 * is executed to drop this relation.If you reload relation after drop, it may
                 * cause other exceptions during the drop process
                 */
            }

            Oid mlogid = find_matview_mlog_table(object->objectId);
            if (mlogid != 0 && !u_sess->attr.attr_sql.enable_cluster_resize) {
                delete_matdep_table(mlogid);
            }

#ifdef PGXC
            /*
             * Do not do extra process if this session is connected to a remote
             * Coordinator.
             */
            if (IsConnFromCoord()) {
                if (NULL != seqname)
                    pfree_ext(seqname);
                break;
            }

            /*
             * This session is connected directly to application, so extra
             * process related to remote nodes and GTM is needed.
             */
            switch (relKind) {
                case RELKIND_SEQUENCE:
                case RELKIND_LARGE_SEQUENCE:
                    /*
                     * Drop the sequence on GTM.
                     * Sequence is dropped on GTM by a remote Coordinator only
                     * for a non temporary sequence.
                     */
                    if (!isTmpSequence) {
                        /*
                         * The sequence has already been removed from Coordinator,
                         * finish the stuff on GTM too
                         */

                        /*
                         * A relation is opened to get the schema and database name as
                         * such data is not available before when dropping a function.
                         */
                        relseq = relation_open(object->objectId, AccessShareLock);
                        int64 seq_uuid = get_uuid_from_rel(relseq);
                        /*
                         * Sequence is not immediately removed on GTM, but at the end
                         * of the transaction block. In case this transaction fails,
                         * all the data remains intact on GTM.
                         */
                        register_sequence_cb(seq_uuid, GTM_DROP_SEQ);

                        /* Delete global sequence */
                        delete_global_seq(object->objectId, relseq);

                        /* Then close the relation opened previously */
                        relation_close(relseq, AccessShareLock);
                    }
                    break;
                case RELKIND_RELATION:
                case RELKIND_CONTQUERY:
                case RELKIND_VIEW:
                    /*
                     * Flag temporary objects in use in case a temporary table or view
                     * is dropped by dependency. This check is particularly useful with
                     * CASCADE when temporary objects are removed by dependency in order
                     * to avoid implicit 2PC would result in an error as temporary
                     * objects cannot be prepared.
                     */
                    if (isTmpTable)
                        ExecSetTempObjectIncluded();
                    break;
                default:
                    break;
            }
#endif /* PGXC */
            break;
        }

        case OCLASS_PROC:
            RemoveFunctionById(object->objectId);
            break;

        case OCLASS_PACKAGE:
            /* if objectId same as objectSubId, it's doing drop package body */
            RemovePackageById(object->objectId, (int32)object->objectId == object->objectSubId);
            break;

        case OCLASS_TYPE:
            RemoveTypeById(object->objectId);
            break;

        case OCLASS_CAST:
            DropCastById(object->objectId);
            break;

        case OCLASS_COLLATION:
            RemoveCollationById(object->objectId);
            break;

        case OCLASS_CONSTRAINT:
            RemoveConstraintById(object->objectId);
            break;

        case OCLASS_CONVERSION:
            RemoveConversionById(object->objectId);
            break;

        case OCLASS_DEFAULT:
            RemoveAttrDefaultById(object->objectId);
            break;

        case OCLASS_LANGUAGE:
            DropProceduralLanguageById(object->objectId);
            break;

        case OCLASS_LARGEOBJECT:
            LargeObjectDrop(object->objectId);
            break;

        case OCLASS_OPERATOR:
            RemoveOperatorById(object->objectId);
            break;

        case OCLASS_OPCLASS:
            RemoveOpClassById(object->objectId);
            break;

        case OCLASS_OPFAMILY:
            RemoveOpFamilyById(object->objectId);
            break;

        case OCLASS_AMOP:
            RemoveAmOpEntryById(object->objectId);
            break;

        case OCLASS_AMPROC:
            RemoveAmProcEntryById(object->objectId);
            break;

        case OCLASS_REWRITE:
            RemoveRewriteRuleById(object->objectId);
            break;

        case OCLASS_TRIGGER:
            RemoveTriggerById(object->objectId);
            break;

        case OCLASS_SCHEMA:
            RemoveSchemaById(object->objectId);
            break;

        case OCLASS_TSPARSER:
            RemoveTSParserById(object->objectId);
            break;

        case OCLASS_TSDICT:
            RemoveTSDictionaryById(object->objectId);
            break;

        case OCLASS_TSTEMPLATE:
            RemoveTSTemplateById(object->objectId);
            break;

        case OCLASS_TSCONFIG:
            RemoveTSConfigurationById(object->objectId);
            break;

            /*
             * OCLASS_ROLE, OCLASS_DATABASE, OCLASS_TBLSPACE intentionally not
             * handled here
             */

        case OCLASS_FDW:
            RemoveForeignDataWrapperById(object->objectId);
            break;

        case OCLASS_FOREIGN_SERVER:
            RemoveForeignServerById(object->objectId);
            break;

        case OCLASS_USER_MAPPING:
            RemoveUserMappingById(object->objectId);
            break;

        case OCLASS_DEFACL:
            RemoveDefaultACLById(object->objectId);
            break;

        case OCLASS_DB_PRIVILEGE:
            DropDbPrivByOid(object->objectId);
            break;

#ifdef PGXC
        case OCLASS_PGXC_CLASS:
            RemovePgxcClass(object->objectId, IS_PGXC_DATANODE);
            /* 
             * pgxc_slice is the extended information for pgxc_class,
             * so need to delete the related tuples in pgxc_slice.
             */
            RemovePgxcSlice(object->objectId);
            break;
#endif

        case OCLASS_EXTENSION:
            RemoveExtensionById(object->objectId);
            break;

        case OCLASS_DATA_SOURCE:
            RemoveDataSourceById(object->objectId);
            break;

        case OCLASS_GLOBAL_SETTING:
            remove_cmk_by_id(object->objectId);
            break;

        case OCLASS_COLUMN_SETTING:
            remove_cek_by_id(object->objectId);
            break;

        case OCLASS_CL_CACHED_COLUMN:
            remove_encrypted_col_by_id(object->objectId);
            break;

        case OCLASS_GLOBAL_SETTING_ARGS:
            remove_cmk_args_by_id(object->objectId);
            break;

        case OCLASS_COLUMN_SETTING_ARGS:
            remove_cek_args_by_id(object->objectId);
            break;

        case OCLASS_DIRECTORY:
            RemoveDirectoryById(object->objectId);
            break;

        case OCLASS_RLSPOLICY:
            RemoveRlsPolicyById(object->objectId);
            break;

        case OCLASS_PG_JOB:
            remove_job_by_oid(object->objectId, RelOid, true);
            break;

        case OCLASS_SYNONYM:
            RemoveSynonymById(object->objectId);
            break;
        case OCLASS_DB4AI_MODEL:
            remove_model_by_oid(object->objectId);
            break;
        case OCLASS_GS_CL_PROC:
            remove_encrypted_proc_by_id(object->objectId);
            break;
        case OCLASS_PUBLICATION:
            RemovePublicationById(object->objectId);
            break;

        case OCLASS_PUBLICATION_REL:
            RemovePublicationRelById(object->objectId);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object class: %u", object->classId)));
    }
}

/*
 * AcquireDeletionLock - acquire a suitable lock for deleting an object
 *
 * We use LockRelation for relations, LockDatabaseObject for everything
 * else.  Note that dependency.c is not concerned with deleting any kind of
 * shared-across-databases object, so we have no need for LockSharedObject.
 */
void AcquireDeletionLock(const ObjectAddress* object, int flags)
{
    if (object->classId == RelationRelationId) {
        /*
         * In DROP INDEX CONCURRENTLY, take only ShareUpdateExclusiveLock on
         * the index for the moment.  index_drop() will promote the lock once
         * it's safe to do so.  In all other cases we need full exclusive
         * lock.
         */
        if (flags & PERFORM_DELETION_CONCURRENTLY)
            LockRelationOid(object->objectId, ShareUpdateExclusiveLock);
        else
            LockRelationOid(object->objectId, AccessExclusiveLock);

        /* we must acquire the base relation lock for matview */
        if (get_rel_relkind(object->objectId) == RELKIND_MATVIEW) {
            Relation matview = heap_open(object->objectId, AccessShareLock);
            if (matview->rd_rules != NULL &&
                matview->rd_rules->numLocks == 1) {
                Query *query = get_matview_query(matview);
                acquire_mativew_tables_lock(query, false);
            }
            heap_close(matview, AccessShareLock);
        }
    } else {
        /* assume we should lock the whole object not a sub-object */
        LockDatabaseObject(object->classId, object->objectId, 0, AccessExclusiveLock);
    }
}

/*
 * ReleaseDeletionLock - release an object deletion lock
 */
static void ReleaseDeletionLock(const ObjectAddress* object)
{
    if (object->classId == RelationRelationId)
        UnlockRelationOid(object->objectId, AccessExclusiveLock);
    else
        /* assume we should lock the whole object not a sub-object */
        UnlockDatabaseObject(object->classId, object->objectId, 0, AccessExclusiveLock);
}

/*
 * recordDependencyOnExpr - find expression dependencies
 *
 * This is used to find the dependencies of rules, constraint expressions,
 * etc.
 *
 * Given an expression or query in node-tree form, find all the objects
 * it refers to (tables, columns, operators, functions, etc).  Record
 * a dependency of the specified type from the given depender object
 * to each object mentioned in the expression.
 *
 * rtable is the rangetable to be used to interpret Vars with varlevelsup=0.
 * It can be NIL if no such variables are expected.
 */
void recordDependencyOnExpr(const ObjectAddress* depender, Node* expr, List* rtable, DependencyType behavior)
{
    find_expr_references_context context;

    context.addrs = new_object_addresses();

    /* Set up interpretation for Vars at varlevelsup = 0 */
    context.rtables = list_make1(rtable);

    /* Scan the expression tree for referenceable objects */
    (void)find_expr_references_walker(expr, &context);

    /* Remove any duplicates */
    eliminate_duplicate_dependencies(context.addrs);

    /* And record 'em */
    recordMultipleDependencies(depender, context.addrs->refs, context.addrs->numrefs, behavior);

    free_object_addresses(context.addrs);
}

/*
 * recordDependencyOnSingleRelExpr - find expression dependencies
 *
 * As above, but only one relation is expected to be referenced (with
 * varno = 1 and varlevelsup = 0).	Pass the relation OID instead of a
 * range table.  An additional frammish is that dependencies on that
 * relation (or its component columns) will be marked with 'self_behavior',
 * whereas 'behavior' is used for everything else.
 *
 * NOTE: the caller should ensure that a whole-table dependency on the
 * specified relation is created separately, if one is needed.	In particular,
 * a whole-row Var "relation.*" will not cause this routine to emit any
 * dependency item.  This is appropriate behavior for subexpressions of an
 * ordinary query, so other cases need to cope as necessary.
 */
void recordDependencyOnSingleRelExpr(
    const ObjectAddress* depender, Node* expr, Oid relId, DependencyType behavior, DependencyType self_behavior)
{
    find_expr_references_context context;
    RangeTblEntry rte;

    context.addrs = new_object_addresses();

    /* We gin up a rather bogus rangetable list to handle Vars */
    errno_t rc = memset_s(&rte, sizeof(rte), 0, sizeof(rte));
    securec_check(rc, "", "");
    rte.type = T_RangeTblEntry;
    rte.rtekind = RTE_RELATION;
    rte.relid = relId;
    rte.relkind = RELKIND_RELATION; /* no need for exactness here */

    context.rtables = list_make1(list_make1(&rte));

    /* Scan the expression tree for referenceable objects */
    (void)find_expr_references_walker(expr, &context);

    /* Remove any duplicates */
    eliminate_duplicate_dependencies(context.addrs);

    /* Separate self-dependencies if necessary */
    if (behavior != self_behavior && context.addrs->numrefs > 0) {
        ObjectAddresses* self_addrs = NULL;
        ObjectAddress* outobj = NULL;
        int oldref, outrefs;

        self_addrs = new_object_addresses();

        outobj = context.addrs->refs;
        outrefs = 0;
        for (oldref = 0; oldref < context.addrs->numrefs; oldref++) {
            ObjectAddress* thisobj = context.addrs->refs + oldref;

            if (thisobj->classId == RelationRelationId && thisobj->objectId == relId) {
                /* Move this ref into self_addrs */
                add_exact_object_address(thisobj, self_addrs);
            } else {
                /* Keep it in context.addrs */
                *outobj = *thisobj;
                outobj++;
                outrefs++;
            }
        }
        context.addrs->numrefs = outrefs;

        /* Record the self-dependencies */
        recordMultipleDependencies(depender, self_addrs->refs, self_addrs->numrefs, self_behavior);

        free_object_addresses(self_addrs);
    }

    /* Record the external dependencies */
    recordMultipleDependencies(depender, context.addrs->refs, context.addrs->numrefs, behavior);

    free_object_addresses(context.addrs);
}

/*
 * Recursively search an expression tree for object references.
 *
 * Note: we avoid creating references to columns of tables that participate
 * in an SQL JOIN construct, but are not actually used anywhere in the query.
 * To do so, we do not scan the joinaliasvars list of a join RTE while
 * scanning the query rangetable, but instead scan each individual entry
 * of the alias list when we find a reference to it.
 *
 * Note: in many cases we do not need to create dependencies on the datatypes
 * involved in an expression, because we'll have an indirect dependency via
 * some other object.  For instance Var nodes depend on a column which depends
 * on the datatype, and OpExpr nodes depend on the operator which depends on
 * the datatype.  However we do need a type dependency if there is no such
 * indirect dependency, as for example in Const and CoerceToDomain nodes.
 *
 * Similarly, we don't need to create dependencies on collations except where
 * the collation is being freshly introduced to the expression.
 */
static bool find_expr_references_walker(Node* node, find_expr_references_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        List* rtable = NIL;
        RangeTblEntry* rte = NULL;

        /* Find matching rtable entry, or complain if not found */
        if (var->varlevelsup >= (Index)list_length(context->rtables))
            ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid varlevelsup %u", var->varlevelsup)));
        rtable = (List*)list_nth(context->rtables, var->varlevelsup);
        if (var->varno <= 0 || var->varno > (Index)list_length(rtable))
            ereport(ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid varno %u", var->varno)));

        rte = rt_fetch(var->varno, rtable);

        /*
         * A whole-row Var references no specific columns, so adds no new
         * dependency.	(We assume that there is a whole-table dependency
         * arising from each underlying rangetable entry.  While we could
         * record such a dependency when finding a whole-row Var that
         * references a relation directly, it's quite unclear how to extend
         * that to whole-row Vars for JOINs, so it seems better to leave the
         * responsibility with the range table.  Note that this poses some
         * risks for identifying dependencies of stand-alone expressions:
         * whole-table references may need to be created separately.)
         */
        if (var->varattno == InvalidAttrNumber)
            return false;
        if (rte->rtekind == RTE_RELATION) {
            /* If it's a plain relation, reference this column */
            add_object_address(OCLASS_CLASS, rte->relid, var->varattno, context->addrs);
        } else if (rte->rtekind == RTE_JOIN) {
            /* Scan join output column to add references to join inputs */
            List* save_rtables = NIL;

            /* We must make the context appropriate for join's level */
            save_rtables = context->rtables;
            context->rtables = list_copy_tail(context->rtables, var->varlevelsup);
            if (var->varattno <= 0 || var->varattno > list_length(rte->joinaliasvars))
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid varattno %d", var->varattno)));
            find_expr_references_walker((Node*)list_nth(rte->joinaliasvars, var->varattno - 1), context);
            list_free_ext(context->rtables);
            context->rtables = save_rtables;
        }
        return false;
    } else if (IsA(node, Const)) {
        Const* con = (Const*)node;
        Oid objoid;

        /* A constant must depend on the constant's datatype */
        add_object_address(OCLASS_TYPE, con->consttype, 0, context->addrs);

        /*
         * We must also depend on the constant's collation: it could be
         * different from the datatype's, if a CollateExpr was const-folded to
         * a simple constant.  However we can save work in the most common
         * case where the collation is "default", since we know that's pinned.
         */
        if (OidIsValid(con->constcollid) && con->constcollid != DEFAULT_COLLATION_OID)
            add_object_address(OCLASS_COLLATION, con->constcollid, 0, context->addrs);

        /*
         * If it's a regclass or similar literal referring to an existing
         * object, add a reference to that object.	(Currently, only the
         * regclass and regconfig cases have any likely use, but we may as
         * well handle all the OID-alias datatypes consistently.)
         */
        if (!con->constisnull) {
            switch (con->consttype) {
                case REGPROCOID:
                case REGPROCEDUREOID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_PROC, objoid, 0, context->addrs);
                    break;
                case REGOPEROID:
                case REGOPERATOROID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(OPEROID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_OPERATOR, objoid, 0, context->addrs);
                    break;
                case REGCLASSOID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_CLASS, objoid, 0, context->addrs);
                    break;
                case REGTYPEOID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_TYPE, objoid, 0, context->addrs);
                    break;
                case REGCONFIGOID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(TSCONFIGOID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_TSCONFIG, objoid, 0, context->addrs);
                    break;
                case REGDICTIONARYOID:
                    objoid = DatumGetObjectId(con->constvalue);
                    if (SearchSysCacheExists1(TSDICTOID, ObjectIdGetDatum(objoid)))
                        add_object_address(OCLASS_TSDICT, objoid, 0, context->addrs);
                    break;
                default:
                    break;
            }
        }
        return false;
    } else if (IsA(node, Param)) {
        Param* param = (Param*)node;

        /* A parameter must depend on the parameter's datatype */
        add_object_address(OCLASS_TYPE, param->paramtype, 0, context->addrs);
        /* and its collation, just as for Consts */
        if (OidIsValid(param->paramcollid) && param->paramcollid != DEFAULT_COLLATION_OID)
            add_object_address(OCLASS_COLLATION, param->paramcollid, 0, context->addrs);
    } else if (IsA(node, FuncExpr)) {
        FuncExpr* funcexpr = (FuncExpr*)node;

        add_object_address(OCLASS_PROC, funcexpr->funcid, 0, context->addrs);

        /*
         * Function may be referenced from one synonym,
         * which is necessary to record the raw dependency before view decoupling.
         */
        if (OidIsValid(funcexpr->refSynOid)) {
            add_object_address(OCLASS_SYNONYM, funcexpr->refSynOid, 0, context->addrs);
        }
        /* fall through to examine arguments */
    } else if (IsA(node, OpExpr)) {
        OpExpr* opexpr = (OpExpr*)node;

        add_object_address(OCLASS_OPERATOR, opexpr->opno, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, DistinctExpr)) {
        DistinctExpr* distinctexpr = (DistinctExpr*)node;

        add_object_address(OCLASS_OPERATOR, distinctexpr->opno, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, NullIfExpr)) {
        NullIfExpr* nullifexpr = (NullIfExpr*)node;

        add_object_address(OCLASS_OPERATOR, nullifexpr->opno, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;

        add_object_address(OCLASS_OPERATOR, opexpr->opno, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, Aggref)) {
        Aggref* aggref = (Aggref*)node;

        add_object_address(OCLASS_PROC, aggref->aggfnoid, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, WindowFunc)) {
        WindowFunc* wfunc = (WindowFunc*)node;

        add_object_address(OCLASS_PROC, wfunc->winfnoid, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, SubPlan)) {
        /* Extra work needed here if we ever need this case */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("already-planned subqueries not supported")));
    } else if (IsA(node, RelabelType)) {
        RelabelType* relab = (RelabelType*)node;

        /* since there is no function dependency, need to depend on type */
        add_object_address(OCLASS_TYPE, relab->resulttype, 0, context->addrs);
        /* the collation might not be referenced anywhere else, either */
        if (OidIsValid(relab->resultcollid) && relab->resultcollid != DEFAULT_COLLATION_OID)
            add_object_address(OCLASS_COLLATION, relab->resultcollid, 0, context->addrs);
    } else if (IsA(node, CoerceViaIO)) {
        CoerceViaIO* iocoerce = (CoerceViaIO*)node;

        /* since there is no exposed function, need to depend on type */
        add_object_address(OCLASS_TYPE, iocoerce->resulttype, 0, context->addrs);
    } else if (IsA(node, ArrayCoerceExpr)) {
        ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;

        if (OidIsValid(acoerce->elemfuncid))
            add_object_address(OCLASS_PROC, acoerce->elemfuncid, 0, context->addrs);
        add_object_address(OCLASS_TYPE, acoerce->resulttype, 0, context->addrs);
        /* fall through to examine arguments */
    } else if (IsA(node, ConvertRowtypeExpr)) {
        ConvertRowtypeExpr* cvt = (ConvertRowtypeExpr*)node;

        /* since there is no function dependency, need to depend on type */
        add_object_address(OCLASS_TYPE, cvt->resulttype, 0, context->addrs);
    } else if (IsA(node, CollateExpr)) {
        CollateExpr* coll = (CollateExpr*)node;

        add_object_address(OCLASS_COLLATION, coll->collOid, 0, context->addrs);
    } else if (IsA(node, RowExpr)) {
        RowExpr* rowexpr = (RowExpr*)node;

        add_object_address(OCLASS_TYPE, rowexpr->row_typeid, 0, context->addrs);
    } else if (IsA(node, RowCompareExpr)) {
        RowCompareExpr* rcexpr = (RowCompareExpr*)node;
        ListCell* l = NULL;

        foreach (l, rcexpr->opnos) {
            add_object_address(OCLASS_OPERATOR, lfirst_oid(l), 0, context->addrs);
        }
        foreach (l, rcexpr->opfamilies) {
            add_object_address(OCLASS_OPFAMILY, lfirst_oid(l), 0, context->addrs);
        }
        /* fall through to examine arguments */
    } else if (IsA(node, CoerceToDomain)) {
        CoerceToDomain* cd = (CoerceToDomain*)node;

        add_object_address(OCLASS_TYPE, cd->resulttype, 0, context->addrs);
    } else if (IsA(node, SortGroupClause)) {
        SortGroupClause* sgc = (SortGroupClause*)node;

        add_object_address(OCLASS_OPERATOR, sgc->eqop, 0, context->addrs);
        if (OidIsValid(sgc->sortop))
            add_object_address(OCLASS_OPERATOR, sgc->sortop, 0, context->addrs);
        return false;
    } else if (IsA(node, Query)) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        Query* query = (Query*)node;
        ListCell* lc = NULL;
        bool result = false;

        /*
         * Add whole-relation refs for each plain relation mentioned in the
         * subquery's rtable, as well as refs for any datatypes and collations
         * used in a RECORD function's output.
         *
         * Note: query_tree_walker takes care of recursing into RTE_FUNCTION
         * RTEs, subqueries, etc, so no need to do that here.  But keep it
         * from looking at join alias lists.
         *
         * Note: we don't need to worry about collations mentioned in
         * RTE_VALUES or RTE_CTE RTEs, because those must just duplicate
         * collations referenced in other parts of the Query.
         */
        foreach (lc, query->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
            ListCell* ct = NULL;

            switch (rte->rtekind) {
                case RTE_RELATION:
                    add_object_address(OCLASS_CLASS, rte->relid, 0, context->addrs);
                    /*
                     * Relation may be referenced from one synonym,
                     * which is necessary to record the raw dependency before view decoupling.
                     */
                    if (OidIsValid(rte->refSynOid)) {
                        add_object_address(OCLASS_SYNONYM, rte->refSynOid, 0, context->addrs);
                    }
                    break;
                case RTE_FUNCTION:
                    foreach (ct, rte->funccoltypes) {
                        add_object_address(OCLASS_TYPE, lfirst_oid(ct), 0, context->addrs);
                    }
                    foreach (ct, rte->funccolcollations) {
                        Oid collid = lfirst_oid(ct);

                        if (OidIsValid(collid) && collid != DEFAULT_COLLATION_OID)
                            add_object_address(OCLASS_COLLATION, collid, 0, context->addrs);
                    }
                    break;
                default:
                    break;
            }
        }

        /*
         * If the query is an INSERT or UPDATE, we should create a dependency
         * on each target column, to prevent the specific target column from
         * being dropped.  Although we will visit the TargetEntry nodes again
         * during query_tree_walker, we won't have enough context to do this
         * conveniently, so do it here.
         */
        if (query->commandType == CMD_INSERT || query->commandType == CMD_UPDATE) {
            RangeTblEntry* rte = NULL;

            if (query->resultRelation <= 0 || query->resultRelation > list_length(query->rtable))
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid resultRelation %d", query->resultRelation)));

            rte = rt_fetch(query->resultRelation, query->rtable);
            if (rte->rtekind == RTE_RELATION) {
                foreach (lc, query->targetList) {
                    TargetEntry* tle = (TargetEntry*)lfirst(lc);

                    if (tle->resjunk)
                        continue; /* ignore junk tlist items */
                    add_object_address(OCLASS_CLASS, rte->relid, tle->resno, context->addrs);
                }
            }
        }

        /*
         * Add dependencies on constraints listed in query's constraintDeps
         */
        foreach (lc, query->constraintDeps) {
            add_object_address(OCLASS_CONSTRAINT, lfirst_oid(lc), 0, context->addrs);
        }

        /* query_tree_walker ignores ORDER BY etc, but we need those opers */
        find_expr_references_walker((Node*)query->sortClause, context);
        find_expr_references_walker((Node*)query->groupClause, context);
        find_expr_references_walker((Node*)query->windowClause, context);
        find_expr_references_walker((Node*)query->distinctClause, context);
        (void)find_expr_references_walker((Node*)query->utilityStmt, context);

        /* Examine substructure of query */
        context->rtables = lcons(query->rtable, context->rtables);
        result =
            query_tree_walker(query, (bool (*)())find_expr_references_walker, (void*)context, QTW_IGNORE_JOINALIASES);
        context->rtables = list_delete_first(context->rtables);
        return result;
    } else if (IsA(node, SetOperationStmt)) {
        SetOperationStmt* setop = (SetOperationStmt*)node;

        /* we need to look at the groupClauses for operator references */
        find_expr_references_walker((Node*)setop->groupClauses, context);
        /* fall through to examine child nodes */
    } else if (IsA(node, CopyStmt)) {
        CopyStmt* stmt = (CopyStmt*)node;
        Assert(stmt->relation);
        Assert(!stmt->query);
        bool is_from = stmt->is_from;

        Oid rel_id = RangeVarGetRelid(stmt->relation, (is_from ? RowExclusiveLock : AccessShareLock), false);
        /* not consider the synonym, copy not support */
        add_object_address(OCLASS_CLASS, rel_id, 0, context->addrs);
        UnlockRelationOid(rel_id, (is_from ? RowExclusiveLock : AccessShareLock));
        return false;
    } else if (IsA(node, AlterTableStmt)) {
        AlterTableStmt* stmt = (AlterTableStmt*)node;
        Assert(stmt->relation);
        Assert(!stmt->cmds);

        /* Open and lock the relation, using the appropriate lock type. */
        Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
        /* not consider the synonym, alter not support */
        add_object_address(OCLASS_CLASS, rel_id, 0, context->addrs);
        UnlockRelationOid(rel_id, AccessShareLock);
        return false;
    }

    return expression_tree_walker(node, (bool (*)())find_expr_references_walker, (void*)context);
}

/*
 * Given an array of dependency references, eliminate any duplicates.
 */
static void eliminate_duplicate_dependencies(ObjectAddresses* addrs)
{
    ObjectAddress* priorobj = NULL;
    int oldref, newrefs;

    /*
     * We can't sort if the array has "extra" data, because there's no way to
     * keep it in sync.  Fortunately that combination of features is not
     * needed.
     */
    Assert(!addrs->extras);

    if (addrs->numrefs <= 1)
        return; /* nothing to do */

    /* Sort the refs so that duplicates are adjacent */
    qsort((void*)addrs->refs, addrs->numrefs, sizeof(ObjectAddress), object_address_comparator);

    /* Remove dups */
    priorobj = addrs->refs;
    newrefs = 1;
    for (oldref = 1; oldref < addrs->numrefs; oldref++) {
        ObjectAddress* thisobj = addrs->refs + oldref;

        if (priorobj->classId == thisobj->classId && priorobj->objectId == thisobj->objectId) {
            if (priorobj->objectSubId == thisobj->objectSubId)
                continue; /* identical, so drop thisobj */

            /*
             * If we have a whole-object reference and a reference to a part
             * of the same object, we don't need the whole-object reference
             * (for example, we don't need to reference both table foo and
             * column foo.bar).  The whole-object reference will always appear
             * first in the sorted list.
             */
            if (priorobj->objectSubId == 0) {
                /* replace whole ref with partial */
                priorobj->objectSubId = thisobj->objectSubId;
                continue;
            }
        }
        /* Not identical, so add thisobj to output set */
        priorobj++;
        *priorobj = *thisobj;
        newrefs++;
    }

    addrs->numrefs = newrefs;
}

/*
 * qsort comparator for ObjectAddress items
 */
static int object_address_comparator(const void* a, const void* b)
{
    const ObjectAddress* obja = (const ObjectAddress*)a;
    const ObjectAddress* objb = (const ObjectAddress*)b;

    if (obja->classId < objb->classId)
        return -1;
    if (obja->classId > objb->classId)
        return 1;
    if (obja->objectId < objb->objectId)
        return -1;
    if (obja->objectId > objb->objectId)
        return 1;

    /*
     * We sort the subId as an unsigned int so that 0 will come first. See
     * logic in eliminate_duplicate_dependencies.
     */
    if ((unsigned int)obja->objectSubId < (unsigned int)objb->objectSubId)
        return -1;
    if ((unsigned int)obja->objectSubId > (unsigned int)objb->objectSubId)
        return 1;
    return 0;
}

/*
 * Routines for handling an expansible array of ObjectAddress items.
 *
 * new_object_addresses: create a new ObjectAddresses array.
 */
ObjectAddresses* new_object_addresses(const int maxRefs)
{
    ObjectAddresses* addrs = NULL;

    addrs = (ObjectAddresses*)palloc(sizeof(ObjectAddresses));

    addrs->numrefs = 0;
    addrs->maxrefs = maxRefs;
    addrs->refs = (ObjectAddress*)palloc(addrs->maxrefs * sizeof(ObjectAddress));
    addrs->extras = NULL; /* until/unless needed */

    return addrs;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * It is convenient to specify the class by ObjectClass rather than directly
 * by catalog OID.
 */
static void add_object_address(ObjectClass oclass, Oid objectId, int32 subId, ObjectAddresses* addrs)
{
    ObjectAddress* item = NULL;

    /* enlarge array if needed */
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress*)repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        Assert(!addrs->extras);
    }
    /* record this item */
    item = addrs->refs + addrs->numrefs;
    item->classId = object_classes[oclass];
    item->objectId = objectId;
    item->objectSubId = subId;
    item->rbDropMode = RB_DROP_MODE_INVALID;
    addrs->numrefs++;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * It is convenient to specify the class by ObjectClass rather than directly
 * by catalog OID.
 */
void add_object_address_ext(Oid classId, Oid objectId, int32 subId, char deptype, ObjectAddresses* addrs)
{
    ObjectAddress* item = NULL;

    /* enlarge array if needed */
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress*)repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        Assert(!addrs->extras);
    }
    /* record this item */
    item = addrs->refs + addrs->numrefs;
    item->classId = classId;
    item->objectId = objectId;
    item->objectSubId = subId;
    item->deptype = deptype;
    item->rbDropMode = RB_DROP_MODE_INVALID;
    addrs->numrefs++;
}

void add_object_address_ext1(const ObjectAddress *obj, ObjectAddresses* addrs)
{
    ObjectAddress* item = NULL;

    /* enlarge array if needed */
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress*)repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        Assert(!addrs->extras);
    }
    /* record this item */
    item = addrs->refs + addrs->numrefs;
    item->classId = obj->classId;
    item->objectId = obj->objectId;
    item->objectSubId = obj->objectSubId;
    item->deptype = obj->deptype;
    item->rbDropMode = obj->rbDropMode;
    addrs->numrefs++;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * As above, but specify entry exactly.
 */
void add_exact_object_address(const ObjectAddress* object, ObjectAddresses* addrs)
{
    ObjectAddress* item = NULL;

    /* enlarge array if needed */
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress*)repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        Assert(!addrs->extras);
    }
    /* record this item */
    item = addrs->refs + addrs->numrefs;
    *item = *object;
    addrs->numrefs++;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * As above, but specify entry exactly and provide some "extra" data too.
 */
static void add_exact_object_address_extra(
    const ObjectAddress* object, const ObjectAddressExtra* extra, ObjectAddresses* addrs)
{
    ObjectAddress* item = NULL;
    ObjectAddressExtra* itemextra = NULL;

    /* allocate extra space if first time */
    if (!addrs->extras)
        addrs->extras = (ObjectAddressExtra*)palloc(addrs->maxrefs * sizeof(ObjectAddressExtra));

    /* enlarge array if needed */
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress*)repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        addrs->extras = (ObjectAddressExtra*)repalloc(addrs->extras, addrs->maxrefs * sizeof(ObjectAddressExtra));
    }
    /* record this item */
    item = addrs->refs + addrs->numrefs;
    *item = *object;
    itemextra = addrs->extras + addrs->numrefs;
    *itemextra = *extra;
    addrs->numrefs++;
}

/*
 * Test whether an object is present in an ObjectAddresses array.
 *
 * We return "true" if object is a subobject of something in the array, too.
 */
bool object_address_present(const ObjectAddress* object, const ObjectAddresses* addrs)
{
    int i;

    for (i = addrs->numrefs - 1; i >= 0; i--) {
        const ObjectAddress* thisobj = addrs->refs + i;

        if (object->classId == thisobj->classId && object->objectId == thisobj->objectId) {
            if (object->objectSubId == thisobj->objectSubId || thisobj->objectSubId == 0)
                return true;
        }
    }

    return false;
}

/*
 * As above, except that if the object is present then also OR the given
 * flags into its associated extra data (which must exist).
 */
static bool object_address_present_add_flags(const ObjectAddress* object, int flags, ObjectAddresses* addrs)
{
    int i;

    for (i = addrs->numrefs - 1; i >= 0; i--) {
        ObjectAddress* thisobj = addrs->refs + i;

        if (object->classId == thisobj->classId && object->objectId == thisobj->objectId) {
            if (object->objectSubId == thisobj->objectSubId) {
                ObjectAddressExtra* thisextra = addrs->extras + i;

                thisextra->flags |= flags;
                return true;
            }
            if (thisobj->objectSubId == 0) {
                /*
                 * We get here if we find a need to delete a column after
                 * having already decided to drop its whole table.	Obviously
                 * we no longer need to drop the column.  But don't plaster
                 * its flags on the table.
                 */
                return true;
            }
        }
    }

    return false;
}

/*
 * Similar to above, except we search an ObjectAddressStack.
 */
static bool stack_address_present_add_flags(const ObjectAddress* object, int flags, ObjectAddressStack* stack)
{
    ObjectAddressStack* stackptr = NULL;

    for (stackptr = stack; stackptr; stackptr = stackptr->next) {
        const ObjectAddress* thisobj = stackptr->object;

        if (object->classId == thisobj->classId && object->objectId == thisobj->objectId) {
            if (object->objectSubId == thisobj->objectSubId) {
                stackptr->flags |= flags;
                return true;
            }

            /*
             * Could visit column with whole table already on stack; this is
             * the same case noted in object_address_present_add_flags(), and
             * as in that case, we don't propagate flags for the component to
             * the whole object.
             */
            if (thisobj->objectSubId == 0)
                return true;
        }
    }

    return false;
}

/*
 * Record multiple dependencies from an ObjectAddresses array, after first
 * removing any duplicates.
 */
void record_object_address_dependencies(
    const ObjectAddress* depender, ObjectAddresses* referenced, DependencyType behavior)
{
    eliminate_duplicate_dependencies(referenced);
    recordMultipleDependencies(depender, referenced->refs, referenced->numrefs, behavior);
}

/*
 * Clean up when done with an ObjectAddresses array.
 */
void free_object_addresses(ObjectAddresses* addrs)
{
    pfree_ext(addrs->refs);
    if (addrs->extras)
        pfree_ext(addrs->extras);
    pfree_ext(addrs);
}

/*
 * Determine the class of a given object identified by objectAddress.
 *
 * This function is essentially the reverse mapping for the object_classes[]
 * table.  We implement it as a function because the OIDs aren't consecutive.
 */
ObjectClass getObjectClass(const ObjectAddress* object)
{
    /* only pg_class entries can have nonzero objectSubId */
    if (object->classId != PackageRelationId && object->classId != RelationRelationId 
                                             && object->objectSubId != 0)
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid objectSubId 0 for object class %u", object->classId)));

    switch (object->classId) {
        case RelationRelationId:
            /* caller must check objectSubId */
            return OCLASS_CLASS;

        case ProcedureRelationId:
            return OCLASS_PROC;

         case PackageRelationId:
            return OCLASS_PACKAGE;
            
        case TypeRelationId:
            return OCLASS_TYPE;

        case CastRelationId:
            return OCLASS_CAST;

        case CollationRelationId:
            return OCLASS_COLLATION;

        case ConstraintRelationId:
            return OCLASS_CONSTRAINT;

        case ConversionRelationId:
            return OCLASS_CONVERSION;

        case AttrDefaultRelationId:
            return OCLASS_DEFAULT;

        case LanguageRelationId:
            return OCLASS_LANGUAGE;

        case LargeObjectRelationId:
            return OCLASS_LARGEOBJECT;

        case OperatorRelationId:
            return OCLASS_OPERATOR;

        case OperatorClassRelationId:
            return OCLASS_OPCLASS;

        case OperatorFamilyRelationId:
            return OCLASS_OPFAMILY;

        case AccessMethodOperatorRelationId:
            return OCLASS_AMOP;

        case AccessMethodProcedureRelationId:
            return OCLASS_AMPROC;

        case RewriteRelationId:
            return OCLASS_REWRITE;

        case TriggerRelationId:
            return OCLASS_TRIGGER;

        case NamespaceRelationId:
            return OCLASS_SCHEMA;

        case TSParserRelationId:
            return OCLASS_TSPARSER;

        case TSDictionaryRelationId:
            return OCLASS_TSDICT;

        case TSTemplateRelationId:
            return OCLASS_TSTEMPLATE;

        case TSConfigRelationId:
            return OCLASS_TSCONFIG;

        case AuthIdRelationId:
            return OCLASS_ROLE;

        case DatabaseRelationId:
            return OCLASS_DATABASE;

        case TableSpaceRelationId:
            return OCLASS_TBLSPACE;

        case ForeignDataWrapperRelationId:
            return OCLASS_FDW;

        case ForeignServerRelationId:
            return OCLASS_FOREIGN_SERVER;

        case UserMappingRelationId:
            return OCLASS_USER_MAPPING;

        case DefaultAclRelationId:
            return OCLASS_DEFACL;

        case DbPrivilegeId:
            return OCLASS_DB_PRIVILEGE;

        case ExtensionRelationId:
            return OCLASS_EXTENSION;
        case PgxcGroupRelationId:
            return OCLASS_PGXC_GROUP;
        case DataSourceRelationId:
            return OCLASS_DATA_SOURCE;
        case PgSynonymRelationId:
            return OCLASS_SYNONYM;
        case RlsPolicyRelationId:
            return OCLASS_RLSPOLICY;
        case PgDirectoryRelationId:
            return OCLASS_DIRECTORY;
        case PgJobRelationId:
            return OCLASS_PG_JOB;

#ifdef PGXC
        case PgxcClassRelationId:
            Assert(object->objectSubId == 0);
            return OCLASS_PGXC_CLASS;
#endif
        case ClientLogicGlobalSettingsId:
            return OCLASS_GLOBAL_SETTING;
        case ClientLogicColumnSettingsId:
            return OCLASS_COLUMN_SETTING;
        case ClientLogicCachedColumnsId:
            return OCLASS_CL_CACHED_COLUMN;
        case ClientLogicGlobalSettingsArgsId:
            return OCLASS_GLOBAL_SETTING_ARGS;
        case ClientLogicColumnSettingsArgsId:
            return OCLASS_COLUMN_SETTING_ARGS;
        case ModelRelationId:
            return OCLASS_DB4AI_MODEL;
        case ClientLogicProcId:
            return OCLASS_GS_CL_PROC;
        case PublicationRelationId:
            return OCLASS_PUBLICATION;

        case PublicationRelRelationId:
            return OCLASS_PUBLICATION_REL;

        case SubscriptionRelationId:
            return OCLASS_SUBSCRIPTION;
        default:
            break;
    }

    /* shouldn't get here */
    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object class: %u", object->classId)));
    return OCLASS_CLASS; /* keep compiler quiet */
}

/*
 * getObjectDescription: build an object description for messages
 *
 * The result is a palloc'd string.
 */
char* getObjectDescription(const ObjectAddress* object)
{
    StringInfoData buffer;

    initStringInfo(&buffer);

    char* signature = NULL;

    switch (getObjectClass(object)) {
        case OCLASS_CLASS:
            getRelationDescription(&buffer, object->objectId);
            if (object->objectSubId != 0)
                appendStringInfo(
                    &buffer, _(" column %s"), get_relid_attribute_name(object->objectId, object->objectSubId));
            break;

        case OCLASS_PROC:
            signature = format_procedure(object->objectId);
            appendStringInfo(&buffer, _("function %s"), signature);
            pfree_ext(signature);
            break;

        case OCLASS_PACKAGE:
            signature = format_procedure(object->objectId);
            appendStringInfo(&buffer, _("package %s"), signature);
            pfree_ext(signature);
            break;

        case OCLASS_TYPE:
            appendStringInfo(&buffer, _("type %s"), format_type_be(object->objectId));
            break;

        case OCLASS_CAST: {
            Relation castDesc;
            ScanKeyData skey[1];
            SysScanDesc rcscan;
            HeapTuple tup;
            Form_pg_cast castForm;

            castDesc = heap_open(CastRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            rcscan = systable_beginscan(castDesc, CastOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(rcscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for cast %u", object->objectId)));

            castForm = (Form_pg_cast)GETSTRUCT(tup);

            appendStringInfo(&buffer,
                _("cast from %s to %s"),
                format_type_be(castForm->castsource),
                format_type_be(castForm->casttarget));

            systable_endscan(rcscan);
            heap_close(castDesc, AccessShareLock);
            break;
        }

        case OCLASS_COLLATION: {
            HeapTuple collTup;
            Form_pg_collation coll;

            collTup = SearchSysCache1(COLLOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(collTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for collation %u", object->objectId)));
            coll = (Form_pg_collation)GETSTRUCT(collTup);
            appendStringInfo(&buffer, _("collation %s"), NameStr(coll->collname));
            ReleaseSysCache(collTup);
            break;
        }

        case OCLASS_CONSTRAINT: {
            HeapTuple conTup;
            Form_pg_constraint con;

            conTup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(conTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for constraint %u", object->objectId)));
            con = (Form_pg_constraint)GETSTRUCT(conTup);

            if (OidIsValid(con->conrelid)) {
                StringInfoData rel;

                initStringInfo(&rel);
                getRelationDescription(&rel, con->conrelid);
                appendStringInfo(&buffer, _("constraint %s on %s"), NameStr(con->conname), rel.data);
                pfree_ext(rel.data);
            } else {
                appendStringInfo(&buffer, _("constraint %s"), NameStr(con->conname));
            }

            ReleaseSysCache(conTup);
            break;
        }

        case OCLASS_CONVERSION: {
            HeapTuple conTup;

            conTup = SearchSysCache1(CONVOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(conTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for conversion %u", object->objectId)));
            appendStringInfo(&buffer, _("conversion %s"), NameStr(((Form_pg_conversion)GETSTRUCT(conTup))->conname));
            ReleaseSysCache(conTup);
            break;
        }

        case OCLASS_DEFAULT: {
            Relation attrdefDesc;
            ScanKeyData skey[1];
            SysScanDesc adscan;
            HeapTuple tup;
            Form_pg_attrdef attrdef;
            ObjectAddress colobject;

            attrdefDesc = heap_open(AttrDefaultRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            adscan = systable_beginscan(attrdefDesc, AttrDefaultOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(adscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for attrdef %u", object->objectId)));

            attrdef = (Form_pg_attrdef)GETSTRUCT(tup);

            colobject.classId = RelationRelationId;
            colobject.objectId = attrdef->adrelid;
            colobject.objectSubId = attrdef->adnum;

            appendStringInfo(&buffer, _("default for %s"), getObjectDescription(&colobject));

            systable_endscan(adscan);
            heap_close(attrdefDesc, AccessShareLock);
            break;
        }

        case OCLASS_LANGUAGE: {
            HeapTuple langTup;

            langTup = SearchSysCache1(LANGOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(langTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for language %u", object->objectId)));
            appendStringInfo(&buffer, _("language %s"), NameStr(((Form_pg_language)GETSTRUCT(langTup))->lanname));
            ReleaseSysCache(langTup);
            break;
        }
        case OCLASS_LARGEOBJECT:
            appendStringInfo(&buffer, _("large object %u"), object->objectId);
            break;

        case OCLASS_OPERATOR:
            appendStringInfo(&buffer, _("operator %s"), format_operator(object->objectId));
            break;

        case OCLASS_OPCLASS: {
            HeapTuple opcTup;
            Form_pg_opclass opcForm;
            HeapTuple amTup;
            Form_pg_am amForm;
            char* nspname = NULL;

            opcTup = SearchSysCache1(CLAOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(opcTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for opclass %u", object->objectId)));
            opcForm = (Form_pg_opclass)GETSTRUCT(opcTup);

            amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opcForm->opcmethod));
            if (!HeapTupleIsValid(amTup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for access method %u", opcForm->opcmethod)));
            amForm = (Form_pg_am)GETSTRUCT(amTup);

            /* Qualify the name if not visible in search path */
            if (OpclassIsVisible(object->objectId))
                nspname = NULL;
            else
                nspname = get_namespace_name(opcForm->opcnamespace);

            appendStringInfo(&buffer,
                _("operator class %s for access method %s"),
                quote_qualified_identifier(nspname, NameStr(opcForm->opcname)),
                NameStr(amForm->amname));

            ReleaseSysCache(amTup);
            ReleaseSysCache(opcTup);
            break;
        }

        case OCLASS_OPFAMILY:
            getOpFamilyDescription(&buffer, object->objectId);
            break;

        case OCLASS_AMOP: {
            Relation amopDesc;
            ScanKeyData skey[1];
            SysScanDesc amscan;
            HeapTuple tup;
            Form_pg_amop amopForm;
            StringInfoData opfam;

            amopDesc = heap_open(AccessMethodOperatorRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            amscan = systable_beginscan(amopDesc, AccessMethodOperatorOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(amscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for amop entry %u", object->objectId)));

            amopForm = (Form_pg_amop)GETSTRUCT(tup);

            initStringInfo(&opfam);
            getOpFamilyDescription(&opfam, amopForm->amopfamily);

            /* ------
               translator: %d is the operator strategy (a number), the
               first two %s's are data type names, the third %s is the
               description of the operator family, and the last %s is the
               textual form of the operator with arguments.  */
            appendStringInfo(&buffer,
                _("operator %d (%s, %s) of %s: %s"),
                amopForm->amopstrategy,
                format_type_be(amopForm->amoplefttype),
                format_type_be(amopForm->amoprighttype),
                opfam.data,
                format_operator(amopForm->amopopr));

            pfree_ext(opfam.data);

            systable_endscan(amscan);
            heap_close(amopDesc, AccessShareLock);
            break;
        }

        case OCLASS_AMPROC: {
            Relation amprocDesc;
            ScanKeyData skey[1];
            SysScanDesc amscan;
            HeapTuple tup;
            Form_pg_amproc amprocForm;
            StringInfoData opfam;

            amprocDesc = heap_open(AccessMethodProcedureRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            amscan = systable_beginscan(amprocDesc, AccessMethodProcedureOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(amscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for amproc entry %u", object->objectId)));

            amprocForm = (Form_pg_amproc)GETSTRUCT(tup);

            initStringInfo(&opfam);
            getOpFamilyDescription(&opfam, amprocForm->amprocfamily);
            signature = format_procedure(amprocForm->amproc);
            /* ------
               translator: %d is the function number, the first two %s's
               are data type names, the third %s is the description of the
               operator family, and the last %s is the textual form of the
               function with arguments.  */
            appendStringInfo(&buffer,
                _("function %d (%s, %s) of %s: %s"),
                amprocForm->amprocnum,
                format_type_be(amprocForm->amproclefttype),
                format_type_be(amprocForm->amprocrighttype),
                opfam.data,
                signature);

            pfree_ext(signature);
            pfree_ext(opfam.data);

            systable_endscan(amscan);
            heap_close(amprocDesc, AccessShareLock);
            break;
        }

        case OCLASS_REWRITE: {
            Relation ruleDesc;
            ScanKeyData skey[1];
            SysScanDesc rcscan;
            HeapTuple tup;
            Form_pg_rewrite rule;

            ruleDesc = heap_open(RewriteRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            rcscan = systable_beginscan(ruleDesc, RewriteOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(rcscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("could not find tuple for rule %u", object->objectId)));

            rule = (Form_pg_rewrite)GETSTRUCT(tup);

            appendStringInfo(&buffer, _("rule %s on "), NameStr(rule->rulename));
            getRelationDescription(&buffer, rule->ev_class);

            systable_endscan(rcscan);
            heap_close(ruleDesc, AccessShareLock);
            break;
        }

        case OCLASS_TRIGGER: {
            Relation trigDesc;
            ScanKeyData skey[1];
            SysScanDesc tgscan;
            HeapTuple tup;
            Form_pg_trigger trig;

            trigDesc = heap_open(TriggerRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            tgscan = systable_beginscan(trigDesc, TriggerOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(tgscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR,  (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for trigger %u", object->objectId)));

            trig = (Form_pg_trigger)GETSTRUCT(tup);

            appendStringInfo(&buffer, _("trigger %s on "), NameStr(trig->tgname));
            getRelationDescription(&buffer, trig->tgrelid);

            systable_endscan(tgscan);
            heap_close(trigDesc, AccessShareLock);
            break;
        }

        case OCLASS_SCHEMA: {
            char* nspname = NULL;

            nspname = get_namespace_name(object->objectId);
            if (nspname == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for namespace %u", object->objectId)));
            appendStringInfo(&buffer, _("schema %s"), nspname);
            break;
        }

        case OCLASS_TSPARSER: {
            HeapTuple tup;

            tup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for text search parser %u", object->objectId)));
            appendStringInfo(
                &buffer, _("text search parser %s"), NameStr(((Form_pg_ts_parser)GETSTRUCT(tup))->prsname));
            ReleaseSysCache(tup);
            break;
        }

        case OCLASS_TSDICT: {
            HeapTuple tup;

            tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for text search dictionary %u", object->objectId)));
            appendStringInfo(
                &buffer, _("text search dictionary %s"), NameStr(((Form_pg_ts_dict)GETSTRUCT(tup))->dictname));
            ReleaseSysCache(tup);
            break;
        }

        case OCLASS_TSTEMPLATE: {
            HeapTuple tup;

            tup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for text search template %u", object->objectId)));
            appendStringInfo(
                &buffer, _("text search template %s"), NameStr(((Form_pg_ts_template)GETSTRUCT(tup))->tmplname));
            ReleaseSysCache(tup);
            break;
        }

        case OCLASS_TSCONFIG: {
            HeapTuple tup;

            tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for text search configuration %u", object->objectId)));
            appendStringInfo(
                &buffer, _("text search configuration %s"), NameStr(((Form_pg_ts_config)GETSTRUCT(tup))->cfgname));
            ReleaseSysCache(tup);
            break;
        }

        case OCLASS_ROLE: {
            appendStringInfo(&buffer, _("role %s"), GetUserNameFromId(object->objectId));
            break;
        }

        case OCLASS_DATABASE: {
            char* datname = NULL;

            datname = get_database_name(object->objectId);

            if (datname == NULL)
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for database %u", object->objectId)));
            appendStringInfo(&buffer, _("database %s"), datname);
            break;
        }

        case OCLASS_TBLSPACE: {
            char* tblspace = NULL;

            tblspace = get_tablespace_name(object->objectId);
            if (tblspace == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for tablespace %u", object->objectId)));
            appendStringInfo(&buffer, _("tablespace %s"), tblspace);
            break;
        }

        case OCLASS_FDW: {
            ForeignDataWrapper* fdw = NULL;

            fdw = GetForeignDataWrapper(object->objectId);
            appendStringInfo(&buffer, _("foreign-data wrapper %s"), fdw->fdwname);
            break;
        }

        case OCLASS_FOREIGN_SERVER: {
            ForeignServer* srv = NULL;

            srv = GetForeignServer(object->objectId);
            appendStringInfo(&buffer, _("server %s"), srv->servername);
            break;
        }

        case OCLASS_USER_MAPPING: {
            HeapTuple tup;
            Oid useid;
            char* usename = NULL;

            tup = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for user mapping %u", object->objectId)));

            useid = ((Form_pg_user_mapping)GETSTRUCT(tup))->umuser;

            ReleaseSysCache(tup);

            if (OidIsValid(useid))
                usename = GetUserNameFromId(useid);
            else
                usename = "public";

            appendStringInfo(&buffer, _("user mapping for %s"), usename);
            break;
        }

        case OCLASS_DATA_SOURCE: {
            DataSource* src = GetDataSource(object->objectId);
            appendStringInfo(&buffer, _("data source %s"), src->srcname);
            break;
        }

        case OCLASS_DEFACL: {
            Relation defaclrel;
            ScanKeyData skey[1];
            SysScanDesc rcscan;
            HeapTuple tup;
            Form_pg_default_acl defacl;

            defaclrel = heap_open(DefaultAclRelationId, AccessShareLock);

            ScanKeyInit(
                &skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));

            rcscan = systable_beginscan(defaclrel, DefaultAclOidIndexId, true, NULL, 1, skey);

            tup = systable_getnext(rcscan);

            if (!HeapTupleIsValid(tup))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find tuple for default ACL %u", object->objectId)));

            defacl = (Form_pg_default_acl)GETSTRUCT(tup);

            switch (defacl->defaclobjtype) {
                case DEFACLOBJ_RELATION:
                    appendStringInfo(&buffer,
                        _("default privileges on new relations belonging to role %s"),
                        GetUserNameFromId(defacl->defaclrole));
                    break;
                case DEFACLOBJ_SEQUENCE:
                    appendStringInfo(&buffer,
                        _("default privileges on new sequences belonging to role %s"),
                        GetUserNameFromId(defacl->defaclrole));
                    break;
                case DEFACLOBJ_FUNCTION:
                    appendStringInfo(&buffer,
                        _("default privileges on new functions belonging to role %s"),
                        GetUserNameFromId(defacl->defaclrole));
                    break;
                case DEFACLOBJ_TYPE:
                    appendStringInfo(&buffer,
                        _("default privileges on new types belonging to role %s"),
                        GetUserNameFromId(defacl->defaclrole));
                    break;
                default:
                    /* shouldn't get here */
                    appendStringInfo(
                        &buffer, _("default privileges belonging to role %s"), GetUserNameFromId(defacl->defaclrole));
                    break;
            }

            if (OidIsValid(defacl->defaclnamespace)) {
                appendStringInfo(&buffer, _(" in schema %s"), get_namespace_name(defacl->defaclnamespace));
            }

            systable_endscan(rcscan);
            heap_close(defaclrel, AccessShareLock);
            break;
        }

        case OCLASS_DB_PRIVILEGE: {
            Relation dbPrivRel = heap_open(DbPrivilegeId, AccessShareLock);
            if (!RelationIsValid(dbPrivRel)) {
                ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Could not open the relation gs_db_privilege."),
                        errcause("System error."), erraction("Contact engineer to support.")));
            }

            HeapTuple dbPrivTuple = SearchSysCache1(DBPRIVOID, ObjectIdGetDatum(object->objectId));
            if (!HeapTupleIsValid(dbPrivTuple)) {
                heap_close(dbPrivRel, AccessShareLock);
                break;
            }
            
            bool isNull = false;
            Datum datum = heap_getattr(
                dbPrivTuple, Anum_gs_db_privilege_privilege_type, RelationGetDescr(dbPrivRel), &isNull);
            appendStringInfo(&buffer, _("\"%s\""), text_to_cstring(DatumGetTextP(datum)));

            ReleaseSysCache(dbPrivTuple);
            heap_close(dbPrivRel, AccessShareLock);
            break;
        }
        case OCLASS_EXTENSION: {
            char* extname = NULL;

            extname = get_extension_name(object->objectId);
            if (extname == NULL)
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for extension %u", object->objectId)));
            appendStringInfo(&buffer, _("extension %s"), extname);
            break;
        }

        case OCLASS_GLOBAL_SETTING:
            get_global_setting_description(&buffer, object);
            break;

        case OCLASS_COLUMN_SETTING:
            get_column_setting_description(&buffer, object);
            break;

        case OCLASS_CL_CACHED_COLUMN:
            get_cached_column_description(&buffer, object);
            break;

        case OCLASS_GLOBAL_SETTING_ARGS:
            get_global_setting_args_description(&buffer, object);
            break;

        case OCLASS_COLUMN_SETTING_ARGS:
            get_column_setting_args_description(&buffer, object);
            break;

        case OCLASS_DIRECTORY: {
            char* directory;

            directory = get_directory_name(object->objectId);
            if (directory == NULL)
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for directory %u", object->objectId)));
            appendStringInfo(&buffer, _("directory %s"), directory);
            break;
        }

        case OCLASS_RLSPOLICY: {
            Relation pg_rlspolicy = heap_open(RlsPolicyRelationId, AccessShareLock);
            /* Use the index to search for the tuple */
            ScanKeyData scanKey[1];
            ScanKeyInit(&scanKey[0],
                ObjectIdAttributeNumber,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(object->objectId));

            SysScanDesc scanDesc = systable_beginscan(pg_rlspolicy, PgRlspolicyOidIndex, true, NULL, 1, scanKey);

            HeapTuple tuple = systable_getnext(scanDesc);
            if (HeapTupleIsValid(tuple) == false) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find tuple for policy %u", object->objectId)));
            }
            Form_pg_rlspolicy rlspolicy = (Form_pg_rlspolicy)GETSTRUCT(tuple);
            /* Construct string info */
            appendStringInfo(&buffer, _("row level security policy %s on "), rlspolicy->polname.data);
            getRelationDescription(&buffer, rlspolicy->polrelid);
            /* Release resource */
            systable_endscan(scanDesc);
            heap_close(pg_rlspolicy, AccessShareLock);
            break;
        }
        case OCLASS_SYNONYM: {
            char* qualifiedSynname = NULL;
            qualifiedSynname = GetQualifiedSynonymName(object->objectId, true);

            if (qualifiedSynname == NULL) {
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for synonym %u", object->objectId)));
            }

            appendStringInfo(&buffer, _("synonym %s"), qualifiedSynname);
            break;
        }
        case OCLASS_PUBLICATION: {
            appendStringInfo(&buffer, _("publication %s"), get_publication_name(object->objectId, false));
            break;
        }

        case OCLASS_PUBLICATION_REL: {
            HeapTuple tup;
            char *pubname;
            Form_pg_publication_rel prform;
            ScanKeyData scanKey[1];
            ScanKeyInit(&scanKey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(object->objectId));
            Relation rel = heap_open(PublicationRelRelationId, AccessShareLock);
            SysScanDesc scanDesc = systable_beginscan(rel, PublicationRelObjectIndexId, true, NULL, 1, scanKey);

            tup = systable_getnext(scanDesc);
            if (!HeapTupleIsValid(tup)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find tuple for publication %u", object->objectId)));
            }

            prform = (Form_pg_publication_rel)GETSTRUCT(tup);
            pubname = get_publication_name(prform->prpubid, false);

            appendStringInfo(&buffer, _("publication table %s in publication %s"), get_rel_name(prform->prrelid),
                pubname);
            systable_endscan(scanDesc);
            heap_close(rel, AccessShareLock);
            break;
        }

        case OCLASS_SUBSCRIPTION: {
            appendStringInfo(&buffer, _("subscription %s"), get_subscription_name(object->objectId, false));
            break;
        }

        default:
            appendStringInfo(
                &buffer, "unrecognized object %u %u %d", object->classId, object->objectId, object->objectSubId);
            break;
    }

    return buffer.data;
}

/*
 * getObjectDescriptionOids: as above, except the object is specified by Oids
 */
char* getObjectDescriptionOids(Oid classid, Oid objid)
{
    ObjectAddress address;

    address.classId = classid;
    address.objectId = objid;
    address.objectSubId = 0;

    return getObjectDescription(&address);
}

/*
 * subroutine for getObjectDescription: describe a relation
 */
static void getRelationDescription(StringInfo buffer, Oid relid)
{
    HeapTuple relTup;
    Form_pg_class relForm;
    char* nspname = NULL;
    char* relname = NULL;

    relTup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relid), LOG);
    if (!HeapTupleIsValid(relTup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    relForm = (Form_pg_class)GETSTRUCT(relTup);

    /* Qualify the name if not visible in search path */
    if (RelationIsVisible(relid))
        nspname = NULL;
    else
        nspname = get_namespace_name(relForm->relnamespace);

    relname = quote_qualified_identifier(nspname, NameStr(relForm->relname));

    switch (relForm->relkind) {
        case RELKIND_RELATION:
            appendStringInfo(buffer, _("table %s"), relname);
            break;
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            appendStringInfo(buffer, _("index %s"), relname);
            break;
        case RELKIND_SEQUENCE:
            appendStringInfo(buffer, _("sequence %s"), relname);
            break;
        case RELKIND_LARGE_SEQUENCE:
            appendStringInfo(buffer, _("large sequence %s"), relname);
            break;
        case RELKIND_TOASTVALUE:
            appendStringInfo(buffer, _("toast table %s"), relname);
            break;
        case RELKIND_VIEW:
            appendStringInfo(buffer, _("view %s"), relname);
            break;
        case RELKIND_CONTQUERY:
            appendStringInfo(buffer, _("contview %s"), relname);
            break;
        case RELKIND_MATVIEW:
            appendStringInfo(buffer, _("materialized view %s"),relname);
            break;
        case RELKIND_COMPOSITE_TYPE:
            appendStringInfo(buffer, _("composite type %s"), relname);
            break;
        case RELKIND_FOREIGN_TABLE:
            appendStringInfo(buffer, _("foreign table %s"), relname);
            break;
        case RELKIND_STREAM:
            appendStringInfo(buffer, _("stream %s"), relname);
            break;
        default:
            /* shouldn't get here */
            appendStringInfo(buffer, _("relation %s"), relname);
            break;
    }

    ReleaseSysCache(relTup);
}

/*
 * subroutine for getObjectDescription: describe an operator family
 */
static void getOpFamilyDescription(StringInfo buffer, Oid opfid)
{
    HeapTuple opfTup;
    Form_pg_opfamily opfForm;
    HeapTuple amTup;
    Form_pg_am amForm;
    char* nspname = NULL;

    opfTup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
    if (!HeapTupleIsValid(opfTup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opfamily %u", opfid)));
    opfForm = (Form_pg_opfamily)GETSTRUCT(opfTup);

    amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
    if (!HeapTupleIsValid(amTup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for access method %u", opfForm->opfmethod)));
    amForm = (Form_pg_am)GETSTRUCT(amTup);

    /* Qualify the name if not visible in search path */
    if (OpfamilyIsVisible(opfid))
        nspname = NULL;
    else
        nspname = get_namespace_name(opfForm->opfnamespace);

    appendStringInfo(buffer,
        _("operator family %s for access method %s"),
        quote_qualified_identifier(nspname, NameStr(opfForm->opfname)),
        NameStr(amForm->amname));

    ReleaseSysCache(amTup);
    ReleaseSysCache(opfTup);
}

/*
 * @Description: Check if there is any objects that depends on the
 *			    special text search configuration
 *
 * @in tsconfoid: special text search configuration
 * @return: true if there is any dependence, else it is false
 */
bool TSConfigurationHasDependentObjects(Oid tsconfoid)
{
    bool ret = false;
    SysScanDesc scan;
    ScanKeyData key[2];
    Relation depRel;
    HeapTuple tuple;

    depRel = heap_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(TSConfigRelationId));

    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tsconfoid));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_depend pg_depend = (Form_pg_depend)GETSTRUCT(tuple);

        /* if there is a relation that depends on the configuration return true */
        if (RelationRelationId == pg_depend->classid) {
            ret = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(depRel, RowExclusiveLock);

    return ret;
}

/*
 * SQL-level callable version of getObjectDescription
 */
Datum pg_describe_object(PG_FUNCTION_ARGS)
{
    Oid classid = PG_GETARG_OID(0);
    Oid objid = PG_GETARG_OID(1);
    int32 subobjid = PG_GETARG_INT32(2);
    char* description = NULL;
    ObjectAddress address;

    /* for "pinned" items in pg_depend, return null */
    if (!OidIsValid(classid) && !OidIsValid(objid))
        PG_RETURN_NULL();

    address.classId = classid;
    address.objectId = objid;
    address.objectSubId = subobjid;

    description = getObjectDescription(&address);
    PG_RETURN_TEXT_P(cstring_to_text(description));
}

#ifdef ENABLE_MULTIPLE_NODES

namespace Tsdb {
/**
 * Used in tsdb. Find dependents for an cudesc table.
 * After calling this function, targetObjects will hold the cudesc table and
 * all the objects that depends on the cudesc table, in proper order.
 * proper order: the deletion order should from the end to the beginning.
 * Parameters:
 *  - targetObjects: holds one cudesc table at first
 *  - depRel: already opened pg_depend relation
 */
static void findTsDependentObjects(ObjectAddresses* targetObjects, Relation depRel)
{
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;
    ObjectAddress otherObj;
    ObjectAddressExtra otherExtra;

    int i = 0;
    while (i < targetObjects->numrefs) {
        ObjectAddress* cur = targetObjects->refs + i;
        otherExtra.dependee = *cur;

        ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(cur->classId));
        ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(cur->objectId));
        scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);
        while(HeapTupleIsValid(tup = systable_getnext(scan))) {
            Form_pg_depend foundDep = (Form_pg_depend)GETSTRUCT(tup);
            otherObj.classId = foundDep->classid;
            otherObj.objectId = foundDep->objid;
            otherObj.objectSubId = foundDep->objsubid;
            /*
             * Must lock the dependent object before recursing to it.
             */
            AcquireDeletionLock(&otherObj, 0);
            /*
            * The dependent object might have been deleted while we waited to
            * lock it; if so, we don't need to do anything more with it. We can
            * test this cheaply and independently of the object's type by seeing
            * if the pg_depend tuple we are looking at is still live. (If the
            * object got deleted, the tuple would have been deleted too.)
            */
            if ((!systable_recheck_tuple(scan, tup))
                || object_address_present(&otherObj, targetObjects)) {
                /* release the now-useless lock */
                ReleaseDeletionLock(&otherObj);
                /* and continue scanning for dependencies */
                continue;
            }
            switch (foundDep->deptype) {
                case DEPENDENCY_AUTO:
                    otherExtra.flags |= DEPFLAG_AUTO;
                    break;
                case DEPENDENCY_INTERNAL:
                    otherExtra.flags |= DEPFLAG_INTERNAL;
                    break;
                default:
                    ereport(LOG, (errmsg(
                        "During findTsDependentObjects, found an dependent with deptype %c. " \
                        "classId|objectId|objectSubId of parent: %u|%u|%d, of dependent: %u|%u|%d.",
                        foundDep->deptype, cur->classId, cur->objectId, cur->objectSubId,
                        otherObj.classId, otherObj.objectId, otherObj.objectSubId)));
                    otherExtra.flags |= DEPFLAG_INTERNAL;
            }
            add_exact_object_address_extra(&otherObj, &otherExtra, targetObjects);
        }
        systable_endscan(scan);
        i++;
    }
}

/**
 * Used in tsdb. Delete cudesc tables and their dependents
 * First, lookup the pg_depend table to find all the dependents,
 * add all the objects into ObjectAddresses.
 * Then, drop objects in proper order by calling deleteOneObject()
 */
void performTsCudescDeletion(List* cudesc_oids)
{
    /**
     * Normally, to delete one cudesc table is to delete 7 objects:
     * cudesc, cudesc_index, toast, toast index in pg_class, and 3 objects in pg_type
     */
    static const int estimate_target_num = 7;
    int n_cudesc = list_length(cudesc_oids);
    Assert(n_cudesc > 0);
    ObjectAddresses* targetObjects = new_object_addresses(estimate_target_num * n_cudesc);

    Relation depRel;
    depRel = heap_open(DependRelationId, RowExclusiveLock);

    ObjectAddress cudescObj;
    ObjectAddressExtra extra;

    cudescObj.classId = RelationRelationId;
    cudescObj.objectSubId = 0;
    extra.flags = DEPFLAG_ORIGINAL;
    ListCell* cell = NULL;
    foreach(cell, cudesc_oids) {
        Oid cudesc_oid = lfirst_oid(cell);
        cudescObj.objectId = cudesc_oid;
        add_exact_object_address_extra(&cudescObj, &extra, targetObjects);
        AcquireDeletionLock(&cudescObj, 0);
    }
    findTsDependentObjects(targetObjects, depRel);

    reportDependentObjects(targetObjects, DROP_CASCADE, DEBUG2, &cudescObj);

    for (int i = targetObjects->numrefs - 1; i >= 0; i--) {
        deleteOneObject(targetObjects->refs + i, &depRel, PERFORM_DELETION_INTERNAL);
    }

    /* clean up */
    free_object_addresses(targetObjects);
    heap_close(depRel, RowExclusiveLock);
}
}
#endif   /* ENABLE_MULTIPLE_NODES */
