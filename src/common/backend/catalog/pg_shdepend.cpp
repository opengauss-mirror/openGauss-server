/* -------------------------------------------------------------------------
 *
 * pg_shdepend.cpp
 *	  routines to support manipulation of the pg_shdepend relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_shdepend.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/gs_package.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/collationcmds.h"
#include "commands/conversioncmds.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/subscriptioncmds.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "commands/directory.h"
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#ifdef ENABLE_MOT
#include "foreign/fdwapi.h"
#endif

typedef enum { LOCAL_OBJECT, SHARED_OBJECT, REMOTE_OBJECT } objectType;

static void getOidListDiff(Oid* list1, int* nlist1, Oid* list2, int* nlist2);
static Oid classIdGetDbId(Oid classId);
static void shdepChangeDep(Relation sdepRel, Oid classid, Oid objid, int32 objsubid, Oid refclassid, Oid refobjid,
    SharedDependencyType deptype);
static void shdepAddDependency(Relation sdepRel, Oid classId, Oid objectId, int32 objsubId, Oid refclassId,
    Oid refobjId, SharedDependencyType deptype, const char* objfile = NULL);
static void shdepDropDependency(Relation sdepRel, Oid classId, Oid objectId, int32 objsubId, bool drop_subobjects,
    Oid refclassId, Oid refobjId, SharedDependencyType deptype);
static void storeObjectDescription(
    StringInfo descs, objectType type, ObjectAddress* object, SharedDependencyType deptype, int count);
static bool isSharedObjectPinned(Oid classId, Oid objectId, Relation sdepRel);

extern void delete_tsfiles(List* objfiles);

/*
 * recordSharedDependencyOn
 *
 * Record a dependency between 2 objects via their respective ObjectAddresses.
 * The first argument is the dependent object, the second the one it
 * references (which must be a shared object).
 *
 * This locks the referenced object and makes sure it still exists.
 * Then it creates an entry in pg_shdepend.  The lock is kept until
 * the end of the transaction.
 *
 * Dependencies on pinned objects are not recorded.
 */
void recordSharedDependencyOn(
    ObjectAddress* depender, ObjectAddress* referenced, SharedDependencyType deptype, const char* objfile)
{
    Relation sdepRel;

    /*
     * Objects in pg_shdepend can't have SubIds.
     */
#ifdef ENABLE_MOT
    Assert(deptype == SHARED_DEPENDENCY_MOT_TABLE || depender->objectSubId == 0);
#else
    Assert(depender->objectSubId == 0);
#endif
    Assert(referenced->objectSubId == 0);

    /*
     * During bootstrap, do nothing since pg_shdepend may not exist yet.
     * initdb will fill in appropriate pg_shdepend entries after bootstrap.
     */
    if (IsBootstrapProcessingMode())
        return;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);
    /* If the referenced object is pinned, do nothing. */
    if (objfile || !isSharedObjectPinned(referenced->classId, referenced->objectId, sdepRel)) {
        shdepAddDependency(sdepRel,
            depender->classId,
            depender->objectId,
            depender->objectSubId,
            referenced->classId,
            referenced->objectId,
            deptype,
            objfile);
    }

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * recordDependencyOnOwner
 *
 * A convenient wrapper of recordSharedDependencyOn -- register the specified
 * user as owner of the given object.
 *
 * Note: it's the caller's responsibility to ensure that there isn't an owner
 * entry for the object already.
 */
void recordDependencyOnOwner(Oid classId, Oid objectId, Oid owner, const char* objfile)
{
    ObjectAddress myself, referenced;

    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = 0;

    referenced.classId = AuthIdRelationId;
    referenced.objectId = owner;
    referenced.objectSubId = 0;

    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_OWNER, objfile);
}

/*
 * recordDependencyOnPackage
 *
 * Register the dependencies to packages in batch.
 */
void recordDependencyOnPackage(Oid classId, Oid objectId, List* pkgOidList)
{
    ObjectAddress myself;
    int nSkip = 0;
    int referencedCnt = 0;
    if ((referencedCnt = list_length(pkgOidList)) == 0) {
        return;
    }

    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = 0;

    ObjectAddress* referenced = (ObjectAddress*)palloc(sizeof(ObjectAddress) * referencedCnt);
    ObjectAddress* cursor = referenced;
    ListCell* lc = NULL;
    foreach(lc, pkgOidList) {
        Oid pkgOid = lfirst_oid(lc);
        if (pkgOid == objectId) {
            nSkip++;
            continue;
        }
        cursor->classId = PackageRelationId;
        cursor->objectId = lfirst_oid(lc);
        cursor->objectSubId = 0;
        cursor++;
    }
    recordMultipleDependencies(&myself, referenced, referencedCnt - nSkip, DEPENDENCY_AUTO);
    pfree_ext(referenced);
}

#ifdef ENABLE_MOT
void recordDependencyOnDatabase(Oid classId, Oid objectId, Oid serverId, Oid database, const char* objfile)
{
    ObjectAddress myself, referenced;

    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = serverId;

    referenced.classId = DatabaseRelationId;
    referenced.objectId = database;
    referenced.objectSubId = 0;

    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_MOT_TABLE, objfile);
}
#endif

/*
 * shdepChangeDep
 *
 * Update shared dependency records to account for an updated referenced
 * object.	This is an internal workhorse for operations such as changing
 * an object's owner.
 *
 * There must be no more than one existing entry for the given dependent
 * object and dependency type!	So in practice this can only be used for
 * updating SHARED_DEPENDENCY_OWNER entries, which should have that property.
 *
 * If there is no previous entry, we assume it was referencing a PINned
 * object, so we create a new entry.  If the new referenced object is
 * PINned, we don't create an entry (and drop the old one, if any).
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
static void shdepChangeDep(Relation sdepRel, Oid classid, Oid objid, int32 objsubid, Oid refclassid, Oid refobjid,
    SharedDependencyType deptype)
{
    Oid dbid = classIdGetDbId(classid);
    HeapTuple oldtup = NULL;
    HeapTuple scantup;
    ScanKeyData key[4];
    SysScanDesc scan;
    bool objfile_isnull = true;

    /*
     * Make sure the new referenced object doesn't go away while we record the
     * dependency.
     */
    shdepLockAndCheckObject(refclassid, refobjid);

    /*
     * Look for a previous entry
     */
    ScanKeyInit(&key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dbid));
    ScanKeyInit(&key[1], Anum_pg_shdepend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
    ScanKeyInit(&key[2], Anum_pg_shdepend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
    ScanKeyInit(&key[3], Anum_pg_shdepend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, 4, key);

    while ((scantup = systable_getnext(scan)) != NULL) {
        /* Ignore if not of the target dependency type */
        if (((Form_pg_shdepend)GETSTRUCT(scantup))->deptype != deptype)
            continue;
        /* Caller screwed up if multiple matches */
        if (oldtup != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("multiple pg_shdepend entries for object %u/%u/%d deptype %c",
                        classid,
                        objid,
                        objsubid,
                        deptype)));
        oldtup = heap_copytuple(scantup);

        (void)heap_getattr(scantup, Anum_pg_shdepend_objfile, RelationGetDescr(sdepRel), &objfile_isnull);
    }

    systable_endscan(scan);

    if (objfile_isnull && isSharedObjectPinned(refclassid, refobjid, sdepRel)) {
        /* No new entry needed, so just delete existing entry if any */
        if (oldtup != NULL)
            simple_heap_delete(sdepRel, &oldtup->t_self);
    } else if (oldtup != NULL) {
        /* Need to update existing entry */
        Form_pg_shdepend shForm = (Form_pg_shdepend)GETSTRUCT(oldtup);

        /* Since oldtup is a copy, we can just modify it in-memory */
        shForm->refclassid = refclassid;
        shForm->refobjid = refobjid;

        simple_heap_update(sdepRel, &oldtup->t_self, oldtup);

        /* keep indexes current */
        CatalogUpdateIndexes(sdepRel, oldtup);
    } else {
        /* Need to insert new entry */
        Datum values[Natts_pg_shdepend];
        bool nulls[Natts_pg_shdepend];

        errno_t rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "", "");

        values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(dbid);
        values[Anum_pg_shdepend_classid - 1] = ObjectIdGetDatum(classid);
        values[Anum_pg_shdepend_objid - 1] = ObjectIdGetDatum(objid);
        values[Anum_pg_shdepend_objsubid - 1] = Int32GetDatum(objsubid);

        values[Anum_pg_shdepend_refclassid - 1] = ObjectIdGetDatum(refclassid);
        values[Anum_pg_shdepend_refobjid - 1] = ObjectIdGetDatum(refobjid);
        values[Anum_pg_shdepend_deptype - 1] = CharGetDatum(deptype);

        nulls[Anum_pg_shdepend_objfile - 1] = true;

        /*
         * we are reusing oldtup just to avoid declaring a new variable, but
         * it's certainly a new tuple
         */
        oldtup = heap_form_tuple(RelationGetDescr(sdepRel), values, nulls);
        (void)simple_heap_insert(sdepRel, oldtup);

        /* keep indexes current */
        CatalogUpdateIndexes(sdepRel, oldtup);
    }

    if (oldtup != NULL)
        heap_freetuple_ext(oldtup);
}

/*
 * changeDependencyOnOwner
 *
 * Update the shared dependencies to account for the new owner.
 *
 * Note: we don't need an objsubid argument because only whole objects
 * have owners.
 */
void changeDependencyOnOwner(Oid classId, Oid objectId, Oid newOwnerId)
{
    Relation sdepRel;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /* Adjust the SHARED_DEPENDENCY_OWNER entry */
    shdepChangeDep(sdepRel, classId, objectId, 0, AuthIdRelationId, newOwnerId, SHARED_DEPENDENCY_OWNER);

    /* ----------
     * There should never be a SHARED_DEPENDENCY_ACL entry for the owner,
     * so get rid of it if there is one.  This can happen if the new owner
     * was previously granted some rights to the object.
     *
     * This step is analogous to aclnewowner's removal of duplicate entries
     * in the ACL.	We have to do it to handle this scenario:
     *		A grants some rights on an object to B
     *		ALTER OWNER changes the object's owner to B
     *		ALTER OWNER changes the object's owner to C
     * The third step would remove all mention of B from the object's ACL,
     * but we'd still have a SHARED_DEPENDENCY_ACL for B if we did not do
     * things this way.
     *
     * The rule against having a SHARED_DEPENDENCY_ACL entry for the owner
     * allows us to fix things up in just this one place, without having
     * to make the various ALTER OWNER routines each know about it.
     * ----------
     */
    shdepDropDependency(sdepRel, classId, objectId, 0, true, AuthIdRelationId, newOwnerId, SHARED_DEPENDENCY_ACL);

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * getOidListDiff
 *		Helper for updateAclDependencies.
 *
 * Takes two Oid arrays and removes elements that are common to both arrays,
 * leaving just those that are in one input but not the other.
 * We assume both arrays have been sorted and de-duped.
 */
static void getOidListDiff(Oid* list1, int* nlist1, Oid* list2, int* nlist2)
{
    int in1, in2, out1, out2;

    in1 = in2 = out1 = out2 = 0;
    while (in1 < *nlist1 && in2 < *nlist2) {
        if (list1[in1] == list2[in2]) {
            /* skip over duplicates */
            in1++;
            in2++;
        } else if (list1[in1] < list2[in2]) {
            /* list1[in1] is not in list2 */
            list1[out1++] = list1[in1++];
        } else {
            /* list2[in2] is not in list1 */
            list2[out2++] = list2[in2++];
        }
    }

    /* any remaining list1 entries are not in list2 */
    while (in1 < *nlist1) {
        list1[out1++] = list1[in1++];
    }

    /* any remaining list2 entries are not in list1 */
    while (in2 < *nlist2) {
        list2[out2++] = list2[in2++];
    }

    *nlist1 = out1;
    *nlist2 = out2;
}

/*
 * updateAclDependencies
 *		Update the pg_shdepend info for an object's ACL during GRANT/REVOKE.
 *
 * classId, objectId, objsubId: identify the object whose ACL this is
 * ownerId: role owning the object
 * noldmembers, oldmembers: array of roleids appearing in old ACL
 * nnewmembers, newmembers: array of roleids appearing in new ACL
 *
 * We calculate the differences between the new and old lists of roles,
 * and then insert or delete from pg_shdepend as appropriate.
 *
 * Note that we can't just insert all referenced roles blindly during GRANT,
 * because we would end up with duplicate registered dependencies.	We could
 * check for existence of the tuples before inserting, but that seems to be
 * more expensive than what we are doing here.	Likewise we can't just delete
 * blindly during REVOKE, because the user may still have other privileges.
 * It is also possible that REVOKE actually adds dependencies, due to
 * instantiation of a formerly implicit default ACL (although at present,
 * all such dependencies should be for the owning role, which we ignore here).
 *
 * NOTE: Both input arrays must be sorted and de-duped.  (Typically they
 * are extracted from an ACL array by aclmembers(), which takes care of
 * both requirements.)	The arrays are pfreed before return.
 */
void updateAclDependencies(Oid classId, Oid objectId, int32 objsubId, Oid ownerId, int noldmembers, Oid* oldmembers,
    int nnewmembers, Oid* newmembers)
{
    Relation sdepRel;
    int i;

    /*
     * Remove entries that are common to both lists; those represent existing
     * dependencies we don't need to change.
     *
     * OK to overwrite the inputs since we'll pfree them anyway.
     */
    getOidListDiff(oldmembers, &noldmembers, newmembers, &nnewmembers);

    if (noldmembers > 0 || nnewmembers > 0) {
        sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

        /* Add new dependencies that weren't already present */
        for (i = 0; i < nnewmembers; i++) {
            Oid roleid = newmembers[i];

            /*
             * Skip the owner: he has an OWNER shdep entry instead. (This is
             * not just a space optimization; it makes ALTER OWNER easier. See
             * notes in changeDependencyOnOwner.)
             */
            if (roleid == ownerId)
                continue;

            /* Skip pinned roles; they don't need dependency entries */
            if (isSharedObjectPinned(AuthIdRelationId, roleid, sdepRel))
                continue;

            shdepAddDependency(sdepRel, classId, objectId, objsubId, AuthIdRelationId, roleid, SHARED_DEPENDENCY_ACL);
        }

        /* Drop no-longer-used old dependencies */
        for (i = 0; i < noldmembers; i++) {
            Oid roleid = oldmembers[i];

            /* Skip the owner, same as above */
            if (roleid == ownerId)
                continue;

            /* Skip pinned roles */
            if (isSharedObjectPinned(AuthIdRelationId, roleid, sdepRel))
                continue;

            shdepDropDependency(sdepRel,
                classId,
                objectId,
                objsubId,
                false, /* exact match on objsubId */
                AuthIdRelationId,
                roleid,
                SHARED_DEPENDENCY_ACL);
        }

        heap_close(sdepRel, RowExclusiveLock);
    }

    if (oldmembers != NULL)
        pfree_ext(oldmembers);
    if (newmembers != NULL)
        pfree_ext(newmembers);
}

/*
 * A struct to keep track of dependencies found in other databases.
 */
typedef struct {
    Oid dbOid;
    int count;
} remoteDep;

SysScanDesc initAndGetScanByRefId(Relation sdepRel, Oid classId, Oid objectId)
{
    const int NumberOfReferenceKey = 2;
    ScanKeyData key[NumberOfReferenceKey];

    errno_t rc = EOK;
    rc = memset_s(key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "", "");

    ScanKeyInit(&key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));

    return systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, NULL, NumberOfReferenceKey, key);
}

SysScanDesc initAndGetScanByDependerIndexId(
    Relation sdepRel, Oid dbId, Oid classId, Oid objectId, const int32 objsubId, const int maxAttributeId)
{
    ScanKeyData key[Anum_pg_shdepend_objsubid];

    errno_t rc = EOK;
    rc = memset_s(key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "", "")

        Assert((maxAttributeId <= Anum_pg_shdepend_objsubid) && (maxAttributeId >= Anum_pg_shdepend_dbid));

    for (int i = 1; i <= maxAttributeId; i++) {
        switch (i) {
            case Anum_pg_shdepend_dbid:
                ScanKeyInit(&key[i - 1], i, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dbId));
                break;
            case Anum_pg_shdepend_classid:
                ScanKeyInit(&key[i - 1], i, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
                break;
            case Anum_pg_shdepend_objid:
                ScanKeyInit(&key[i - 1], i, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));
                break;
            case Anum_pg_shdepend_objsubid:
                ScanKeyInit(&key[i - 1], i, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubId));
                break;
            default:
                break;
        }
    }

    return systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, maxAttributeId, key);
}

/*
 * checkSharedDependencies
 *
 * Check whether there are shared dependency entries for a given shared
 * object; return true if so.
 *
 * In addition, return a string containing a newline-separated list of object
 * descriptions that depend on the shared object, or NULL if none is found.
 * We actually return two such strings; the "detail" result is suitable for
 * returning to the client as an errdetail() string, and is limited in size.
 * The "detail_log" string is potentially much longer, and should be emitted
 * to the server log only.
 *
 * We can find three different kinds of dependencies: dependencies on objects
 * of the current database; dependencies on shared objects; and dependencies
 * on objects local to other databases.  We can (and do) provide descriptions
 * of the two former kinds of objects, but we can't do that for "remote"
 * objects, so we just provide a count of them.
 *
 * If we find a SHARED_DEPENDENCY_PIN entry, we can error out early.
 */
bool checkSharedDependencies(Oid classId, Oid objectId, char** detail_msg, char** detail_log_msg)
{
    Relation sdepRel;
    SysScanDesc scan;
    HeapTuple tup;
    int numReportedDeps = 0;
    int numNotReportedDeps = 0;
    int numNotReportedDbs = 0;
    List* remDeps = NIL;
    ListCell* cell = NULL;
    ObjectAddress object;
    StringInfoData descs;
    StringInfoData alldescs;

    /*
     * We limit the number of dependencies reported to the client to
     * MAX_REPORTED_DEPS, since client software may not deal well with
     * enormous error strings.	The server log always gets a full report.
     */
#define MAX_REPORTED_DEPS 100

    initStringInfo(&descs);
    initStringInfo(&alldescs);

    sdepRel = heap_open(SharedDependRelationId, AccessShareLock);
    scan = initAndGetScanByRefId(sdepRel, classId, objectId);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_shdepend sdepForm = (Form_pg_shdepend)GETSTRUCT(tup);
        /* This case can be dispatched quickly */
        if (sdepForm->deptype == SHARED_DEPENDENCY_PIN) {
            object.classId = classId;
            object.objectId = objectId;
            object.objectSubId = 0;
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop %s because it is required by the database system",
                        getObjectDescription(&object))));
        }

        object.classId = sdepForm->classid;
        object.objectId = sdepForm->objid;
        object.objectSubId = sdepForm->objsubid;

        /*
         * If it's a dependency local to this database or it's a shared
         * object, describe it.
         *
         * If it's a remote dependency, keep track of it so we can report the
         * number of them later.
         */
        if (sdepForm->dbid == u_sess->proc_cxt.MyDatabaseId) {
            if (numReportedDeps < MAX_REPORTED_DEPS) {
                numReportedDeps++;
                storeObjectDescription(&descs, LOCAL_OBJECT, &object, (SharedDependencyType)sdepForm->deptype, 0);
            } else
                numNotReportedDeps++;
            storeObjectDescription(&alldescs, LOCAL_OBJECT, &object, (SharedDependencyType)sdepForm->deptype, 0);
        } else if (sdepForm->dbid == InvalidOid) {
            if (numReportedDeps < MAX_REPORTED_DEPS) {
                numReportedDeps++;
                storeObjectDescription(&descs, SHARED_OBJECT, &object, (SharedDependencyType)sdepForm->deptype, 0);
            } else
                numNotReportedDeps++;
            storeObjectDescription(&alldescs, SHARED_OBJECT, &object, (SharedDependencyType)sdepForm->deptype, 0);
        } else {
            /* It's not local nor shared, so it must be remote. */
            remoteDep* dep = NULL;
            bool stored = false;

            /*
             * XXX this info is kept on a simple List.	Maybe it's not good
             * for performance, but using a hash table seems needlessly
             * complex.  The expected number of databases is not high anyway,
             * I suppose.
             */
            foreach (cell, remDeps) {
                dep = (remoteDep*)lfirst(cell);
                if (dep->dbOid == sdepForm->dbid) {
                    dep->count++;
                    stored = true;
                    break;
                }
            }
            if (!stored) {
                dep = (remoteDep*)palloc(sizeof(remoteDep));
                dep->dbOid = sdepForm->dbid;
                dep->count = 1;
                remDeps = lappend(remDeps, dep);
            }
        }
    }

    systable_endscan(scan);

    heap_close(sdepRel, AccessShareLock);

    /*
     * Summarize dependencies in remote databases.
     */
    foreach (cell, remDeps) {
        remoteDep* dep = (remoteDep*)lfirst(cell);

        object.classId = DatabaseRelationId;
        object.objectId = dep->dbOid;
        object.objectSubId = 0;

        if (numReportedDeps < MAX_REPORTED_DEPS) {
            numReportedDeps++;
            storeObjectDescription(&descs, REMOTE_OBJECT, &object, SHARED_DEPENDENCY_INVALID, dep->count);
        } else
            numNotReportedDbs++;
        storeObjectDescription(&alldescs, REMOTE_OBJECT, &object, SHARED_DEPENDENCY_INVALID, dep->count);
    }

    list_free_deep(remDeps);

    if (descs.len == 0) {
        pfree_ext(descs.data);
        pfree_ext(alldescs.data);
        *detail_msg = *detail_log_msg = NULL;
        return false;
    }

    if (numNotReportedDeps > 0)
        appendStringInfo(&descs,
            ngettext("\nand %d other object "
                     "(see server log for list)",
                "\nand %d other objects "
                "(see server log for list)",
                numNotReportedDeps),
            numNotReportedDeps);
    if (numNotReportedDbs > 0)
        appendStringInfo(&descs,
            ngettext("\nand objects in %d other database "
                     "(see server log for list)",
                "\nand objects in %d other databases "
                "(see server log for list)",
                numNotReportedDbs),
            numNotReportedDbs);

    *detail_msg = descs.data;
    *detail_log_msg = alldescs.data;
    return true;
}

/*
 * copyTemplateDependencies
 *
 * Routine to create the initial shared dependencies of a new database.
 * We simply copy the dependencies from the template database.
 */
void copyTemplateDependencies(Oid templateDbId, Oid newDbId)
{
    Relation sdepRel;
    TupleDesc sdepDesc;
    ScanKeyData key[1];
    SysScanDesc scan;
    HeapTuple tup;
    CatalogIndexState indstate;
    Datum values[Natts_pg_shdepend];
    bool nulls[Natts_pg_shdepend];
    bool replace[Natts_pg_shdepend];
    errno_t rc = EOK;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);
    sdepDesc = RelationGetDescr(sdepRel);

    indstate = CatalogOpenIndexes(sdepRel);

    /* Scan all entries with dbid = templateDbId */
    ScanKeyInit(&key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(templateDbId));

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, 1, key);

    /* Set up to copy the tuples except for inserting newDbId */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(replace, sizeof(replace), false, sizeof(replace));
    securec_check(rc, "", "");

    replace[Anum_pg_shdepend_dbid - 1] = true;
    values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(newDbId);

    /*
     * Copy the entries of the original database, changing the database Id to
     * that of the new database.  Note that because we are not copying rows
     * with dbId == 0 (ie, rows describing dependent shared objects) we won't
     * copy the ownership dependency of the template database itself; this is
     * what we want.
     */
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        HeapTuple newtup;

        newtup = heap_modify_tuple(tup, sdepDesc, values, nulls, replace);
        (void)simple_heap_insert(sdepRel, newtup);

        /* Keep indexes current */
        CatalogIndexInsert(indstate, newtup);

        heap_freetuple_ext(newtup);
    }

    systable_endscan(scan);

    CatalogCloseIndexes(indstate);
    heap_close(sdepRel, RowExclusiveLock);
}

#ifdef ENABLE_MOT
static void MotFdwDropDatabaseDependency(HeapTuple tup)
{
    Form_pg_shdepend shdepForm = (Form_pg_shdepend)GETSTRUCT(tup);
    if (shdepForm->deptype == SHARED_DEPENDENCY_MOT_TABLE && shdepForm->objsubid != 0) {
        FdwRoutine* fdwRoutine = GetFdwRoutineByServerId(shdepForm->objsubid);
        /* Forward drop stmt to MOT FDW. */
        if (fdwRoutine != NULL && fdwRoutine->GetFdwType != NULL && fdwRoutine->GetFdwType() == MOT_ORC &&
            fdwRoutine->ValidateTableDef != NULL) {
            DropForeignStmt stmt;
            stmt.type = T_DropForeignStmt;
            stmt.relkind = RELKIND_RELATION;
            stmt.reloid = shdepForm->objid;
            stmt.indexoid = 0;
            stmt.name = "#FROM DROP DB#";
            fdwRoutine->ValidateTableDef((Node*)&stmt);
        }
    }
}
#endif

/*
 * dropDatabaseDependencies
 *
 * Delete pg_shdepend entries corresponding to a database that's being
 * dropped.
 */
void dropDatabaseDependencies(Oid databaseId)
{
    Relation sdepRel;
    ScanKeyData key[1];
    SysScanDesc scan;
    HeapTuple tup;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /*
     * First, delete all the entries that have the database Oid in the dbid
     * field.
     */
    ScanKeyInit(&key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(databaseId));
    /* We leave the other index fields unspecified */
    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, 1, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
#ifdef ENABLE_MOT
        /* Forward drop stmt to MOT FDW. */
        MotFdwDropDatabaseDependency(tup);
#endif
        simple_heap_delete(sdepRel, &tup->t_self);
    }

    systable_endscan(scan);

    /* Now delete all entries corresponding to the database itself */
    shdepDropDependency(
        sdepRel, DatabaseRelationId, databaseId, 0, true, InvalidOid, InvalidOid, SHARED_DEPENDENCY_INVALID);

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * deleteSharedDependencyRecordsFor
 *
 * Delete all pg_shdepend entries corresponding to an object that's being
 * dropped or modified.  The object is assumed to be either a shared object
 * or local to the current database (the classId tells us which).
 *
 * If objectSubId is zero, we are deleting a whole object, so get rid of
 * pg_shdepend entries for subobjects as well.
 */
void deleteSharedDependencyRecordsFor(Oid classId, Oid objectId, int32 objectSubId)
{
    Relation sdepRel;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    shdepDropDependency(
        sdepRel, classId, objectId, objectSubId, (objectSubId == 0), InvalidOid, InvalidOid, SHARED_DEPENDENCY_INVALID);

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * shdepAddDependency
 *		Internal workhorse for inserting into pg_shdepend
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
static void shdepAddDependency(Relation sdepRel, Oid classId, Oid objectId, int32 objsubId, Oid refclassId,
    Oid refobjId, SharedDependencyType deptype, const char* objfile)
{
    HeapTuple tup;
    Datum values[Natts_pg_shdepend];
    bool nulls[Natts_pg_shdepend];

    /*
     * Make sure the object doesn't go away while we record the dependency on
     * it.	DROP routines should lock the object exclusively before they check
     * shared dependencies.
     */
    shdepLockAndCheckObject(refclassId, refobjId);

    errno_t rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");

    /*
     * Form the new tuple and record the dependency.
     */
    values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(classIdGetDbId(classId));
    values[Anum_pg_shdepend_classid - 1] = ObjectIdGetDatum(classId);
    values[Anum_pg_shdepend_objid - 1] = ObjectIdGetDatum(objectId);
    values[Anum_pg_shdepend_objsubid - 1] = Int32GetDatum(objsubId);

    values[Anum_pg_shdepend_refclassid - 1] = ObjectIdGetDatum(refclassId);
    values[Anum_pg_shdepend_refobjid - 1] = ObjectIdGetDatum(refobjId);
    values[Anum_pg_shdepend_deptype - 1] = CharGetDatum(deptype);

    if (objfile != NULL) {
        values[Anum_pg_shdepend_objfile - 1] = CStringGetTextDatum(objfile);
    } else {
        nulls[Anum_pg_shdepend_objfile - 1] = true;
    }

    tup = heap_form_tuple(sdepRel->rd_att, values, nulls);

    (void)simple_heap_insert(sdepRel, tup);

    /* keep indexes current */
    CatalogUpdateIndexes(sdepRel, tup);

    /* clean up */
    heap_freetuple_ext(tup);
}

/*
 * shdepDropDependency
 *		Internal workhorse for deleting entries from pg_shdepend.
 *
 * We drop entries having the following properties:
 *	dependent object is the one identified by classId/objectId/objsubId
 *	if refclassId isn't InvalidOid, it must match the entry's refclassid
 *	if refobjId isn't InvalidOid, it must match the entry's refobjid
 *	if deptype isn't SHARED_DEPENDENCY_INVALID, it must match entry's deptype
 *
 * If drop_subobjects is true, we ignore objsubId and consider all entries
 * matching classId/objectId.
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
static void shdepDropDependency(Relation sdepRel, Oid classId, Oid objectId, int32 objsubId, bool drop_subobjects,
    Oid refclassId, Oid refobjId, SharedDependencyType deptype)
{
    ScanKeyData key[4];
    int nkeys;
    SysScanDesc scan;
    HeapTuple tup;

    /* Scan for entries matching the dependent object */
    ScanKeyInit(
        &key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classIdGetDbId(classId)));
    ScanKeyInit(&key[1], Anum_pg_shdepend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&key[2], Anum_pg_shdepend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));
    if (drop_subobjects) {
        nkeys = 3;
    } else {
        ScanKeyInit(&key[3], Anum_pg_shdepend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubId));
        nkeys = 4;
    }

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_shdepend shdepForm = (Form_pg_shdepend)GETSTRUCT(tup);
        /* Filter entries according to additional parameters */
        if (OidIsValid(refclassId) && shdepForm->refclassid != refclassId)
            continue;
        if (OidIsValid(refobjId) && shdepForm->refobjid != refobjId)
            continue;
        if (deptype != SHARED_DEPENDENCY_INVALID && shdepForm->deptype != deptype)
            continue;

        /* OK, delete it */
        simple_heap_delete(sdepRel, &tup->t_self);
    }

    systable_endscan(scan);
}

/*
 * classIdGetDbId
 *
 * Get the database Id that should be used in pg_shdepend, given the OID
 * of the catalog containing the object.  For shared objects, it's 0
 * (InvalidOid); for all other objects, it's the current database Id.
 */
static Oid classIdGetDbId(Oid classId)
{
    Oid dbId;

    if (IsSharedRelation(classId))
        dbId = InvalidOid;
    else
        dbId = u_sess->proc_cxt.MyDatabaseId;

    return dbId;
}

/*
 * shdepLockAndCheckObject
 *
 * Lock the object that we are about to record a dependency on.
 * After it's locked, verify that it hasn't been dropped while we
 * weren't looking.  If the object has been dropped, this function
 * does not return!
 */
void shdepLockAndCheckObject(Oid classId, Oid objectId)
{
    /* AccessShareLock should be OK, since we are not modifying the object */
    LockSharedObject(classId, objectId, 0, AccessShareLock);

    switch (classId) {
        case AuthIdRelationId:
            if (!SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(objectId)))
                ereport(
                    ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role %u was concurrently dropped", objectId)));
            break;

            /*
             * Currently, this routine need not support any other shared
             * object types besides roles.	If we wanted to record explicit
             * dependencies on databases or tablespaces, we'd need code along
             * these lines:
             */
#ifdef NOT_USED
        case TableSpaceRelationId: {
            /* For lack of a syscache on pg_tablespace, do this: */
            char* tablespace = get_tablespace_name(objectId);

            if (tablespace == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace %u was concurrently dropped", objectId)));
            pfree_ext(tablespace);
            break;
        }
#endif

        case DatabaseRelationId: {
            /* For lack of a syscache on pg_database, do this: */
            char* database = get_and_check_db_name(objectId, true);
            pfree_ext(database);
            break;
        }
        case ResourcePoolRelationId: {
            /* For lack of a syscache on pg_resource_pool, do this: */
            char* pool_name = get_resource_pool_name(objectId);

            if (pool_name == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("resource pool %u was concurrently dropped", objectId)));
            pfree_ext(pool_name);
            break;
        }

        default:
            ereport(
                ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized shared classId: %u", classId)));
    }
}

/*
 * storeObjectDescription
 *		Append the description of a dependent object to "descs"
 *
 * While searching for dependencies of a shared object, we stash the
 * descriptions of dependent objects we find in a single string, which we
 * later pass to ereport() in the DETAIL field when somebody attempts to
 * drop a referenced shared object.
 *
 * When type is LOCAL_OBJECT or SHARED_OBJECT, we expect object to be the
 * dependent object, deptype is the dependency type, and count is not used.
 * When type is REMOTE_OBJECT, we expect object to be the database object,
 * and count to be nonzero; deptype is not used in this case.
 */
static void storeObjectDescription(
    StringInfo descs, objectType type, ObjectAddress* object, SharedDependencyType deptype, int count)
{
    char* objdesc = getObjectDescription(object);

    /* separate entries with a newline */
    if (descs->len != 0)
        appendStringInfoChar(descs, '\n');

    switch (type) {
        case LOCAL_OBJECT:
        case SHARED_OBJECT:
            if (deptype == SHARED_DEPENDENCY_OWNER)
                appendStringInfo(descs, _("owner of %s"), objdesc);
            else if (deptype == SHARED_DEPENDENCY_ACL)
                appendStringInfo(descs, _("privileges for %s"), objdesc);
            else if (deptype == SHARED_DEPENDENCY_RLSPOLICY)
                appendStringInfo(descs, _("target of %s"), objdesc);
            else if (deptype == SHARED_DEPENDENCY_DBPRIV)
                appendStringInfo(descs, _("privileges for %s"), objdesc);
            else
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized dependency type: %d", (int)deptype)));
            break;

        case REMOTE_OBJECT:
            /* translator: %s will always be "database %s" */
            appendStringInfo(descs, ngettext("%d object in %s", "%d objects in %s", count), count, objdesc);
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object type: %d", type)));
    }

    pfree_ext(objdesc);
}

/*
 * isSharedObjectPinned
 *		Return whether a given shared object has a SHARED_DEPENDENCY_PIN entry.
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
static bool isSharedObjectPinned(Oid classId, Oid objectId, Relation sdepRel)
{
    bool result = false;
    SysScanDesc scan;
    HeapTuple tup;
    bool isNull = false;

    scan = initAndGetScanByRefId(sdepRel, classId, objectId);

    /*
     * Since we won't generate additional pg_shdepend entries for pinned
     * objects, there can be at most one entry referencing a pinned object.
     * Hence, it's sufficient to look at the first returned tuple; we need
     * to loop, just to skip include objfile tuple.
     */
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        (void)heap_getattr(tup, Anum_pg_shdepend_objfile, RelationGetDescr(sdepRel), &isNull);

        /* Here need skip include objfile tuple, because this is forced insert. */
        if (!isNull) {
            continue;
        }

        Form_pg_shdepend shdepForm = (Form_pg_shdepend)GETSTRUCT(tup);
        if (shdepForm->deptype == SHARED_DEPENDENCY_PIN)
            result = true;

        break;
    }

    systable_endscan(scan);

    return result;
}

/*
 * shdepDropOwned
 *
 * Drop the objects owned by any one of the given RoleIds.	If a role has
 * access to an object, the grant will be removed as well (but the object
 * will not, of course).
 *
 * We can revoke grants immediately while doing the scan, but drops are
 * saved up and done all at once with performMultipleDeletions.  This
 * is necessary so that we don't get failures from trying to delete
 * interdependent objects in the wrong order.
 */
void shdepDropOwned(List* roleids, DropBehavior behavior)
{
    Relation sdepRel;
    ListCell* cell = NULL;
    ObjectAddresses* deleteobjs = NULL;

    deleteobjs = new_object_addresses();

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock failures.
     */
    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /*
     * For each role, find the dependent objects and drop them using the
     * regular (non-shared) dependency management.
     */
    foreach (cell, roleids) {
        Oid roleid = lfirst_oid(cell);
        SysScanDesc scan;
        HeapTuple tuple;

        /* Doesn't work for pinned objects */
        if (isSharedObjectPinned(AuthIdRelationId, roleid, sdepRel)) {
            ObjectAddress obj;

            obj.classId = AuthIdRelationId;
            obj.objectId = roleid;
            obj.objectSubId = 0;

            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop objects owned by %s because they are "
                           "required by the database system",
                        getObjectDescription(&obj))));
        }

        scan = initAndGetScanByRefId(sdepRel, AuthIdRelationId, roleid);

        while ((tuple = systable_getnext(scan)) != NULL) {
            Form_pg_shdepend sdepForm = (Form_pg_shdepend)GETSTRUCT(tuple);
            ObjectAddress obj;

            /*
             * We only operate on shared objects and objects in the current
             * database
             */
            if (sdepForm->dbid != u_sess->proc_cxt.MyDatabaseId && sdepForm->dbid != InvalidOid)
                continue;

            switch (sdepForm->deptype) {
                    /* Shouldn't happen */
                case SHARED_DEPENDENCY_PIN:
                case SHARED_DEPENDENCY_INVALID:
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("unexpected dependency type")));
                    break;
                case SHARED_DEPENDENCY_ACL:
                    RemoveRoleFromObjectACL(roleid, sdepForm->classid, sdepForm->objid);
                    break;
                case SHARED_DEPENDENCY_RLSPOLICY:
                    /* If unable to remove role from rlspolicy, remove rlspolicy. */
                    if (!RemoveRoleFromRlsPolicy(roleid, sdepForm->classid, sdepForm->objid)) {
                        obj.classId = sdepForm->classid;
                        obj.objectId = sdepForm->objid;
                        obj.objectSubId = sdepForm->objsubid;
                        add_exact_object_address(&obj, deleteobjs);
                    }
                    break;
                case SHARED_DEPENDENCY_OWNER:
                    /* If a local object, save it for deletion below */
                    if (sdepForm->dbid == u_sess->proc_cxt.MyDatabaseId) {
                        obj.classId = sdepForm->classid;
                        obj.objectId = sdepForm->objid;
                        obj.objectSubId = sdepForm->objsubid;
                        add_exact_object_address(&obj, deleteobjs);
                    }
                    break;
                case SHARED_DEPENDENCY_DBPRIV:
                    if (sdepForm->dbid == u_sess->proc_cxt.MyDatabaseId) {
                        obj.classId = sdepForm->classid;
                        obj.objectId = sdepForm->objid;
                        obj.objectSubId = sdepForm->objsubid;
                        add_exact_object_address(&obj, deleteobjs);
                    }
                    break;
                default:
                    break;
            }
        }

        systable_endscan(scan);
    }

    /* the dependency mechanism does the actual work */
    performMultipleDeletions(deleteobjs, behavior, 0);

    heap_close(sdepRel, RowExclusiveLock);

    free_object_addresses(deleteobjs);
}

/*
 * shdepReassignOwned
 *
 * Change the owner of objects owned by any of the roles in roleids to
 * newrole.  Grants are not touched.
 */
void shdepReassignOwned(List* roleids, Oid newrole)
{
    Relation sdepRel;
    ListCell* cell = NULL;

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock problems.
     */
    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    foreach (cell, roleids) {
        SysScanDesc scan;
        HeapTuple tuple;
        Oid roleid = lfirst_oid(cell);
        /* Refuse to work on pinned roles */
        if (isSharedObjectPinned(AuthIdRelationId, roleid, sdepRel)) {
            ObjectAddress obj;

            obj.classId = AuthIdRelationId;
            obj.objectId = roleid;
            obj.objectSubId = 0;

            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot reassign ownership of objects owned by %s because they are required by the database "
                           "system",
                        getObjectDescription(&obj))));

            /*
             * There's no need to tell the whole truth, which is that we
             * didn't track these dependencies at all ...
             */
        }

        scan = initAndGetScanByRefId(sdepRel, AuthIdRelationId, roleid);

        while ((tuple = systable_getnext(scan)) != NULL) {
            Form_pg_shdepend sdepForm = (Form_pg_shdepend)GETSTRUCT(tuple);
            /* We only operate on objects in the current database */
            if (sdepForm->dbid != u_sess->proc_cxt.MyDatabaseId)
                continue;

            /* Unexpected because we checked for pins above */
            if (sdepForm->deptype == SHARED_DEPENDENCY_PIN)
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("unexpected shared pin")));

            /* We leave non-owner dependencies alone */
            if (sdepForm->deptype != SHARED_DEPENDENCY_OWNER)
                continue;

            /* Issue the appropriate ALTER OWNER call */
            switch (sdepForm->classid) {
                case CollationRelationId:
                    AlterCollationOwner_oid(sdepForm->objid, newrole);
                    break;

                case ConversionRelationId:
                    AlterConversionOwner_oid(sdepForm->objid, newrole);
                    break;

                case TypeRelationId:
                    AlterTypeOwnerInternal(sdepForm->objid, newrole, true);
                    break;

                case OperatorRelationId:
                    AlterOperatorOwner_oid(sdepForm->objid, newrole);
                    break;

                case NamespaceRelationId:
                    AlterSchemaOwner_oid(sdepForm->objid, newrole);
                    break;

                case RelationRelationId:

                    /*
                     * Pass recursing = true so that we don't fail on indexes,
                     * owned sequences, etc when we happen to visit them
                     * before their parent table.
                     */
                    ATExecChangeOwner(sdepForm->objid, newrole, true, AccessExclusiveLock);
                    break;

                case ProcedureRelationId:
                    AlterFunctionOwner_oid(sdepForm->objid, newrole);
                    break;

                case LanguageRelationId:
                    AlterLanguageOwner_oid(sdepForm->objid, newrole);
                    break;

                case LargeObjectRelationId:
                    LargeObjectAlterOwner(sdepForm->objid, newrole);
                    break;

                case DefaultAclRelationId:

                    /*
                     * Ignore default ACLs; they should be handled by DROP
                     * OWNED, not REASSIGN OWNED.
                     */
                    break;

                case OperatorClassRelationId:
                    AlterOpClassOwner_oid(sdepForm->objid, newrole);
                    break;

                case OperatorFamilyRelationId:
                    AlterOpFamilyOwner_oid(sdepForm->objid, newrole);
                    break;

                case ForeignServerRelationId:
                    AlterForeignServerOwner_oid(sdepForm->objid, newrole);
                    break;

                case ForeignDataWrapperRelationId:
                    AlterForeignDataWrapperOwner_oid(sdepForm->objid, newrole);
                    break;

                case EventTriggerRelationId:
                    AlterEventTriggerOwner_oid(sdepForm->objid, newrole);
                    break;
                    
                case ExtensionRelationId:
                    AlterExtensionOwner_oid(sdepForm->objid, newrole);
                    break;

                case TSDictionaryRelationId:
                    AlterTSDictionaryOwner_oid(sdepForm->objid, newrole);
                    break;

                case PublicationRelationId:
                    AlterPublicationOwner_oid(sdepForm->objid, newrole);
                    break;

                case SubscriptionRelationId:
                    AlterSubscriptionOwner_oid(sdepForm->objid, newrole);
                    break;

                case PgDirectoryRelationId:
                    AlterPgDirectoryOwner_oid(sdepForm->objid, newrole);
                    break;

                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected classid %u", sdepForm->classid)));
                    break;
            }
            /* Make sure the next iteration will see my changes */
            CommandCounterIncrement();
        }

        systable_endscan(scan);
    }

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * recordDependencyOnRespool
 *
 * A convenient wrapper of recordSharedDependencyOn -- register the specified
 * resource pool as owner of the given object.
 *
 * Note: it's the caller's responsibility to ensure that there isn't an owner
 * entry for the object already.
 */
void recordDependencyOnRespool(Oid classId, Oid objectId, Oid owner)
{
    ObjectAddress myself, referenced;

    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = 0;

    referenced.classId = ResourcePoolRelationId;
    referenced.objectId = owner;
    referenced.objectSubId = 0;

    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_OWNER);
}

/*
 * CheckDependencyOnRespool
 *
 * Get list of objid with 'classId' and refrenced by 'refobjectId'
 * if full is true, get the full list in the foundlist,
 * if full is false, check if there is objid dependent on refobjectId,
 * get only one and return result.
 */
bool CheckDependencyOnRespool(Oid classId, Oid refobjectId, List** foundlist, bool full)
{
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    Relation sdepRel = heap_open(SharedDependRelationId, AccessShareLock);
    bool result = false;

    ScanKeyInit(
        &key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ResourcePoolRelationId));
    ScanKeyInit(&key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(refobjectId));

    scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_shdepend sdepForm = (Form_pg_shdepend)GETSTRUCT(tup);
        if (sdepForm->classid == classId && OidIsValid(sdepForm->objid)) {
            if (!result)
                result = true;

            if (foundlist != NULL)
                *foundlist = lappend_oid(*foundlist, sdepForm->objid);

            if (!full) {
                systable_endscan(scan);
                heap_close(sdepRel, AccessShareLock);
                return result;
            }
        }
    }
    systable_endscan(scan);

    heap_close(sdepRel, AccessShareLock);

    return result;
}
/*
 * changeDependencyOnRespool
 *
 * change 'objectId' with 'classId' dependent on new onwer of resource pool
 * 'newOwnerId'
 */
void changeDependencyOnRespool(Oid classId, Oid objectId, Oid newOwnerId)
{
    Relation sdepRel = NULL;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /* Adjust the SHARED_DEPENDENCY_OWNER entry */
    shdepChangeDep(sdepRel, classId, objectId, 0, ResourcePoolRelationId, newOwnerId, SHARED_DEPENDENCY_OWNER);

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * @Description: Insert use-defined C function library file into PendingLibraryDelete
 * when drop database.
 * @in databaseId: Database oid which will be deleted.
 */
void prepareDatabaseCFunLibrary(Oid databaseId)
{
    Relation sdepRel = NULL;
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    bool isNull = true;

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /* Search pg_shdepend, append library to PendingLibraryDeletes. */
    scan = initAndGetScanByDependerIndexId(sdepRel, databaseId, ProcedureRelationId, 0, 0, Anum_pg_shdepend_classid);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Datum probin = heap_getattr(tup, Anum_pg_shdepend_objfile, RelationGetDescr(sdepRel), &isNull);

        if (!isNull) {
            char* probinstring = TextDatumGetCString(probin);
            InsertIntoPendingLibraryDelete(probinstring, true);
        }
    }

    systable_endscan(scan);

    heap_close(sdepRel, RowExclusiveLock);
}

/*
 * @Description: Delete internal dictionary files if drop text search dictionary.
 * @in dictId: text search dictionary oid.
 * @out: void
 */
void deleteDictionaryTSFile(Oid dictId)
{
    Relation sdepRel = NULL;
    HeapTuple scantup = NULL;
    SysScanDesc scan = NULL;
    List* objfiles = NIL;
    Oid classId = TSDictionaryRelationId;
    Oid dbId = classIdGetDbId(classId);
    Oid objectId = dictId;
    const int32 objsubId = 0;
    SharedDependencyType deptype = SHARED_DEPENDENCY_OWNER;

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete tsfile for dictionary %u", objectId)));

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    scan = initAndGetScanByDependerIndexId(sdepRel, dbId, classId, objectId, objsubId, Anum_pg_shdepend_objsubid);

    while ((scantup = systable_getnext(scan)) != NULL) {
        /* Ignore if not of the target dependency type */
        if (((Form_pg_shdepend)GETSTRUCT(scantup))->deptype != deptype)
            continue;

        /* Caller screwed up if multiple matches */
        if (objfiles != NIL)
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("multiple pg_shdepend entries: object %u/%u/%u/%d, deptype %c",
                        dbId,
                        classId,
                        objectId,
                        objsubId,
                        deptype)));

        bool isNull = true;
        Datum objfile = heap_getattr(scantup, Anum_pg_shdepend_objfile, RelationGetDescr(sdepRel), &isNull);
        if (!isNull)
            objfiles = lappend(objfiles, TextDatumGetCString(objfile));
    }

    systable_endscan(scan);
    heap_close(sdepRel, RowExclusiveLock);

    /* We need to drop internal dictionary files */
    if (objfiles != NIL) {
        delete_tsfiles(objfiles);
        list_free_deep(objfiles);
    }
}

/*
 * @Description: Delete internal dictionary files when drop database.
 * @in databaseId: database id.
 * @out: void
 */
void deleteDatabaseTSFile(Oid databaseId)
{
    Relation sdepRel = NULL;
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    bool isNull = true;
    List* objfiles = NIL;

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete tsfile for database %u", databaseId)));

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /* Search pg_shdepend */
    scan = initAndGetScanByDependerIndexId(sdepRel, databaseId, TSDictionaryRelationId, 0, 0, Anum_pg_shdepend_classid);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Datum objfile = heap_getattr(tup, Anum_pg_shdepend_objfile, RelationGetDescr(sdepRel), &isNull);
        if (!isNull)
            objfiles = lappend(objfiles, TextDatumGetCString(objfile));
    }

    systable_endscan(scan);
    heap_close(sdepRel, RowExclusiveLock);

    /* We need to drop internal dictionary files */
    if (objfiles != NIL) {
        delete_tsfiles(objfiles);
        list_free_deep(objfiles);
    }
}

/*
 * @Description: Update pg_shdepend's objfile for alter text search dictionary.
 * @in objectId: objid in pg_shdepend
 * @in refobjId: refobjid in pg_shdepend
 * @in newObjfile: new value of objfile
 * @out: void
 */
void changeDependencyOnObjfile(Oid objectId, Oid refobjId, const char* newObjfile)
{
    HeapTuple oldtup = NULL;
    HeapTuple scantup = NULL;
    SysScanDesc scan = NULL;
    Relation sdepRel = NULL;
    Oid classId = TSDictionaryRelationId;
    Oid dbId = classIdGetDbId(classId);
    const int32 objsubId = 0;
    Oid refclassId = AuthIdRelationId;
    SharedDependencyType deptype = SHARED_DEPENDENCY_OWNER;

    ereport(DEBUG2,
        (errmodule(MOD_TS),
            errmsg("Update pg_shdepend objfile: object %u/%u/%u/%d, deptype %c, new objfile %s",
                dbId,
                classId,
                objectId,
                objsubId,
                deptype,
                newObjfile)));

    sdepRel = heap_open(SharedDependRelationId, RowExclusiveLock);

    /*
     * Make sure the new referenced object doesn't go away while we record the
     * dependency.
     */
    shdepLockAndCheckObject(refclassId, refobjId);

    /*
     * Look for a previous entry
     */
    scan = initAndGetScanByDependerIndexId(sdepRel, dbId, classId, objectId, objsubId, Anum_pg_shdepend_objsubid);

    while ((scantup = systable_getnext(scan)) != NULL) {
        /* Ignore if not of the target dependency type */
        if (((Form_pg_shdepend)GETSTRUCT(scantup))->deptype != deptype)
            continue;

        /* Caller screwed up if multiple matches */
        if (oldtup != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("multiple pg_shdepend entries for object %u/%u/%u/%d deptype %c",
                        dbId,
                        classId,
                        objectId,
                        objsubId,
                        deptype)));

        oldtup = heap_copytuple(scantup);
    }

    systable_endscan(scan);

    bool ispinned = isSharedObjectPinned(refclassId, refobjId, sdepRel);
    errno_t rc = EOK;
    if (oldtup == NULL) {
        if (newObjfile == NULL) {
            /* must have one entry for not-pinned object */
            if (!ispinned)
                ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("no pg_shdepend entry: object %u/%u/%u/%d, deptype %c",
                            dbId,
                            classId,
                            objectId,
                            objsubId,
                            deptype)));
        } else {
            /* No old entry, have to add a new one */
            HeapTuple newtup;
            Datum values[Natts_pg_shdepend];
            bool nulls[Natts_pg_shdepend];
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "", "");

            /*
             * Form the new tuple and record the dependency.
             */
            values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(dbId);
            values[Anum_pg_shdepend_classid - 1] = ObjectIdGetDatum(classId);
            values[Anum_pg_shdepend_objid - 1] = ObjectIdGetDatum(objectId);
            values[Anum_pg_shdepend_objsubid - 1] = Int32GetDatum(objsubId);
            values[Anum_pg_shdepend_refclassid - 1] = ObjectIdGetDatum(refclassId);
            values[Anum_pg_shdepend_refobjid - 1] = ObjectIdGetDatum(refobjId);
            values[Anum_pg_shdepend_deptype - 1] = CharGetDatum(deptype);
            values[Anum_pg_shdepend_objfile - 1] = CStringGetTextDatum(newObjfile);

            newtup = heap_form_tuple(sdepRel->rd_att, values, nulls);
            (void)simple_heap_insert(sdepRel, newtup);

            /* keep indexes current */
            CatalogUpdateIndexes(sdepRel, newtup);

            /* clean up */
            heap_freetuple_ext(newtup);
        }
    } else {
        if (ispinned && newObjfile == NULL) {
            /* No entry needed, just delete the existing entry */
            simple_heap_delete(sdepRel, &oldtup->t_self);
        } else {
            /* Update the existing entry */
            Datum repl_val[Natts_pg_shdepend];
            bool repl_null[Natts_pg_shdepend];
            bool repl_repl[Natts_pg_shdepend];
            HeapTuple newtup;

            rc = memset_s(repl_val, sizeof(repl_val), 0, sizeof(repl_val));
            securec_check(rc, "", "");
            rc = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
            securec_check(rc, "", "");
            rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
            securec_check(rc, "", "");

            if (newObjfile != NULL)
                repl_val[Anum_pg_shdepend_objfile - 1] = CStringGetTextDatum(newObjfile);
            else
                repl_null[Anum_pg_shdepend_objfile - 1] = true;
            repl_repl[Anum_pg_shdepend_objfile - 1] = true;

            newtup = heap_modify_tuple(oldtup, RelationGetDescr(sdepRel), repl_val, repl_null, repl_repl);

            simple_heap_update(sdepRel, &newtup->t_self, newtup);

            /* keep indexes current */
            CatalogUpdateIndexes(sdepRel, newtup);

            heap_freetuple_ext(newtup);
        }
    }

    /* Clean up */
    heap_close(sdepRel, RowExclusiveLock);
    if (oldtup != NULL)
        heap_freetuple_ext(oldtup);
}
