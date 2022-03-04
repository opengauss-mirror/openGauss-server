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
 * directory.cpp
 *	  Commands for creating and dropping directory

 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/directory.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_directory.h"
#include "commands/directory.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#endif
#include "storage/lmgr.h"

/*
 * @@GaussDB@@
 * Brief		: Check if a directory can be added as entry.
 * Description	: only absolute path is allowed, and three phases to check path legality
 * Notes		:
 */
static void directory_path_check(CreateDirectoryStmt* stmt, char* dirpath)
{
    /* Three phases to check path legality */
    /* Step 1: check if the directory path name has any special forbidden characters or sensitive words,it should be
     * absolute path */
    if (!is_absolute_path(dirpath)) {
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_NAME),
            errmsg("directory path cannot be relative"),
            errdetail("N/A"), errcause("relative directory path is not supported"),
            erraction("substitute relative path by absolute path")));
    }

    const char* danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`","\\","\'","'",
        "\"","{","}","(",")","[","]","~","*","?","!","\n", "pg_audit", NULL};

    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(dirpath, danger_character_list[i]) != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME),
                    errmsg("directory path contains illegal string: \"%s\"", danger_character_list[i])));
        }
    }

    /* Step 2: check if the directory exists, if not, create one */
    struct stat st;              /* directory attribute */
    if (lstat(dirpath, &st) < 0) {
        /* path does not exist */
        ereport(WARNING,
            (errmsg("could not get \"%s\" status, directory does not exist, must make sure directory existance before "
                    "using",
                dirpath)));
    } else {
        /* Step 3: if path exists, check legality */
        if (!S_ISDIR(st.st_mode)) {
            /* check if it is a regular dir, if it is not, error */
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a directory, please check", dirpath)));
        } else if (S_ISLNK(st.st_mode)) {
            /* check if it is a symlink, if it is, error */
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is a symlink, cannot be added as directory", dirpath)));
        } else if (-1 == access(dirpath, X_OK | R_OK | W_OK)) {
            /* check if has permissions, if not, error */
            ereport(WARNING,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("Permission denied for \"%s\", do not have full permissions (Read/Write/Exec) to this "
                           "directory",
                        dirpath)));
        }
    }
}

/*
 * Check permission for create directory.
 */
static void CheckCreateDirectoryPermission()
{
    /*
     * When enable_access_server_directory is off, only initial user can create directory.
     * When enable_access_server_directory is on, sysadmin and the member of gs_role_directory_create role
     * can create directory.
     */
    if (g_instance.attr.attr_storage.enable_access_server_directory) {
        if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_DIRECTORY_CREATE)) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create directory"),
                    errhint("must be sysadmin or a member of the gs_role_directory_create role "
                        "to create a directory")));
        }
    } else {
        if (!initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create directory"),
                    errhint("must be initial user to create a directory")));
        }
    }
}

/*
 * @@GaussDB@@
 * Brief		: Create a directory.
 * Description	: Only superusers can create a directory. This seems a reasonable
 *				  restriction since we're determining the system layout and,
 *				  anyway, we probably have root if we're doing this kind of activity.
 * Notes		:
 */
void CreatePgDirectory(CreateDirectoryStmt* stmt)
{
    Relation rel;
    Datum values[Natts_pg_directory];
    bool nulls[Natts_pg_directory];
    bool replaces[Natts_pg_directory];
    Oid directoryId = InvalidOid;
    char* location = NULL;
    Oid ownerId = InvalidOid;
    Oid targetoid = InvalidOid;
    HeapTuple oldtup = NULL;
    HeapTuple tup = NULL;
    TupleDesc tupDesc = NULL;
    errno_t rc;

    /* Permission check. */
    CheckCreateDirectoryPermission();

    /* get the current user id */
    ownerId = GetUserId();

    /* unix-ify the offered path, and strip any trailing slashes */
    location = pstrdup(stmt->location);
    canonicalize_path(location);
    directory_path_check(stmt, location);

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(values, sizeof(values), (Datum)0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_directory_directory_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->directoryname));
    values[Anum_pg_directory_directory_path - 1] = CStringGetTextDatum(location);
    values[Anum_pg_directory_owner - 1] = ObjectIdGetDatum(ownerId);
    nulls[Anum_pg_directory_directory_acl - 1] = true;
    /*
     * Check that there is no other directory by this name. If exists same name
     * directory and the replace mode is true, it will update the real directory
     * path.
     */
    /* insert or update tuple into pg_directory */
    rel = heap_open(PgDirectoryRelationId, RowExclusiveLock);
    targetoid = get_directory_oid(stmt->directoryname, true);
    LockDatabaseObject(PgDirectoryRelationId, targetoid, 0, AccessExclusiveLock);
    if (OidIsValid(targetoid)) {
        /* existing a entry before */
        if (stmt->replace) {
            replaces[Anum_pg_directory_directory_name - 1] = false;
            oldtup = SearchSysCache1(DIRECTORYOID, targetoid);
            tupDesc = RelationGetDescr(rel);
            if (!HeapTupleIsValid(oldtup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for directory %u", targetoid)));
            }
            tup = (HeapTuple) tableam_tops_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
            simple_heap_update(rel, &tup->t_self, tup);

            ReleaseSysCache(oldtup);
            tableam_tops_free_tuple(tup);
            pfree(location);
            heap_close(rel, RowExclusiveLock);
        } else {
            pfree(location);
            heap_close(rel, RowExclusiveLock);
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("directory \"%s\" is already used by an existing object", stmt->directoryname)));
        }
    } else {
        /* create a new entry */
        tup = heap_form_tuple(rel->rd_att, values, nulls);
        directoryId = simple_heap_insert(rel, tup);
        CatalogUpdateIndexes(rel, tup);
        tableam_tops_free_tuple(tup);

        /* Record dependency on owner */
        recordDependencyOnOwner(PgDirectoryRelationId, directoryId, ownerId);
        pfree(location);
        heap_close(rel, RowExclusiveLock);
    }
}

/*
 * Check permission for drop directory.
 */
static void CheckDropDirectoryPermission(Oid directoryId, const char* directoryName)
{
    /*
     * When enable_access_server_directory is off, only initial user can drop directory,
     * When enable_access_server_directory is on, directory owner or users have drop privileges of the directory or
     * the member of the gs_role_directory_drop role can drop directory.
     */
    if (g_instance.attr.attr_storage.enable_access_server_directory) {
        AclResult aclresult = pg_directory_aclcheck(directoryId, GetUserId(), ACL_DROP);
        if (aclresult != ACLCHECK_OK && !superuser() && !pg_directory_ownercheck(directoryId, GetUserId())
            && !is_member_of_role(GetUserId(), DEFAULT_ROLE_DIRECTORY_DROP)) {
            aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DIRECTORY, directoryName);
        }
    } else {
        if (!initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to drop directory \"%s\"", directoryName),
                    errhint("must be initial user to drop a directory")));
        }
    }
}

/*
 * @@GaussDB@@
 * Brief		: Drop a directory.
 * Description	: Drop a directory by its name. If the directory exists, then
 *				  drop it.
 */
void DropPgDirectory(DropDirectoryStmt* stmt)
{
    TableScanDesc scandesc = NULL;
    Relation rel;
    HeapTuple tuple = NULL;
    ScanKeyData entry[1];
    Oid directoryId = InvalidOid;

    /* find the target tuple */
    rel = heap_open(PgDirectoryRelationId, RowExclusiveLock);

    ScanKeyInit(&entry[0],
        Anum_pg_directory_directory_name,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(stmt->directoryname));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple)  tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    if (HeapTupleIsValid(tuple))
        directoryId = HeapTupleGetOid(tuple);
    else
        directoryId = InvalidOid;

    if (OidIsValid(directoryId)) {
        /* Permission check. */
        CheckDropDirectoryPermission(directoryId, stmt->directoryname);

        /* Remove the pg_directory tuple (this will roll back if we fail below) */
        simple_heap_delete(rel, &tuple->t_self);
        /* Remove dependency on owner. */
        deleteSharedDependencyRecordsFor(PgDirectoryRelationId, directoryId, 0);
        tableam_scan_end(scandesc);
        heap_close(rel, RowExclusiveLock);
    } else {
        tableam_scan_end(scandesc);
        heap_close(rel, RowExclusiveLock);
        if (!stmt->missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("directory \"%s\" does not exist", stmt->directoryname)));
        else
            ereport(NOTICE, (errmsg("directory \"%s\" does not exist, skipping", stmt->directoryname)));
    }
}

/*
 * @@GaussDB@@
 * Brief		: Given a directory name, look up the OID.
 * Description	: If missing_ok is false, throw an error if directory name not
 *				  found. If true, just return InvalidOid.
 */
Oid get_directory_oid(const char* directoryname, bool missing_ok)
{
    Oid result = InvalidOid;
    Relation rel;
    TableScanDesc scandesc = NULL;
    HeapTuple tuple = NULL;
    ScanKeyData entry[1];

    /* search the pg_directory to find the directory */
    rel = heap_open(PgDirectoryRelationId, AccessShareLock);

    ScanKeyInit(
        &entry[0], Anum_pg_directory_directory_name, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(directoryname));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
        result = HeapTupleGetOid(tuple);
    else
        result = InvalidOid;

    tableam_scan_end(scandesc);
    heap_close(rel, AccessShareLock);

    if (!OidIsValid(result) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("directory \"%s\" does not exist", directoryname)));

    return result;
}

/*
 * @@GaussDB@@
 * Brief		: Given a directory OID, look up its name.
 * Description	:
 */
char* get_directory_name(Oid dir_oid)
{
    char* result = NULL;
    Relation rel;
    TableScanDesc scandesc = NULL;
    HeapTuple tuple = NULL;
    ScanKeyData entry[1];

    /* search pg_directory */
    rel = heap_open(PgDirectoryRelationId, AccessShareLock);

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dir_oid));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
        result = pstrdup(NameStr(((Form_pg_directory)GETSTRUCT(tuple))->dirname));
    else
        result = NULL;

    tableam_scan_end(scandesc);
    heap_close(rel, AccessShareLock);

    return result;
}

/*
 * Guts of directory deletion.
 */
void RemoveDirectoryById(Oid dirOid)
{
    Relation relation;
    HeapTuple tup = NULL;

    relation = heap_open(PgDirectoryRelationId, RowExclusiveLock);

    tup = SearchSysCache1(DIRECTORYOID, ObjectIdGetDatum(dirOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for directory %u", dirOid)));

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * ALTER Directory name OWNER TO newowner
 */
void AlterDirectoryOwner(const char* dirname, Oid newOwnerId)
{
    HeapTuple tuple = NULL;
    Relation rel;
    ScanKeyData scankey;
    SysScanDesc scan = NULL;
    Form_pg_directory dirForm = NULL;

    /*
     * Get the old tuple.  We don't need a lock on the directory per se,
     * because we're not going to do anything that would mess up incoming
     * connections.
     */
    rel = heap_open(PgDirectoryRelationId, RowExclusiveLock);
    ScanKeyInit(&scankey, Anum_pg_directory_directory_name, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(dirname));
    scan = systable_beginscan(rel, PgDirectoryDirectoriesNameIndexId, true, NULL, 1, &scankey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(rel, RowExclusiveLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("directory \"%s\" does not exist", dirname)));
    }
    dirForm = (Form_pg_directory)GETSTRUCT(tuple);
    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is to be consistent with other
     * objects.
     */
    if (dirForm->owner != newOwnerId) {
        Datum repl_val[Natts_pg_directory];
        bool repl_null[Natts_pg_directory];
        bool repl_repl[Natts_pg_directory];
        Acl* newAcl = NULL;
        Datum aclDatum;
        bool isNull = false;
        HeapTuple newtuple;
        errno_t rc;

        if (g_instance.attr.attr_storage.enable_access_server_directory) {
            /* must be sysadmin or owner of the existing object */
            if (!superuser() && !pg_directory_ownercheck(HeapTupleGetOid(tuple), GetUserId())) {
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DIRECTORY, dirname);
            }
        } else {
            if (!initialuser()) {
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to change owner of directory"),
                        errhint("must be initial user to change owner of a directory")));
            }
        }

        /* Must be able to become new owner */
        check_is_member_of_role(GetUserId(), newOwnerId);

        rc = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
        securec_check(rc, "\0", "\0");
        rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(rc, "\0", "\0");

        repl_repl[Anum_pg_directory_owner - 1] = true;
        repl_val[Anum_pg_directory_owner - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = tableam_tops_tuple_getattr(tuple, Anum_pg_directory_directory_acl, RelationGetDescr(rel), &isNull);
        if (!isNull) {
            newAcl = aclnewowner(DatumGetAclP(aclDatum), dirForm->owner, newOwnerId);
            repl_repl[Anum_pg_directory_directory_acl - 1] = true;
            repl_val[Anum_pg_directory_directory_acl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = (HeapTuple) tableam_tops_modify_tuple(tuple, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);

        tableam_tops_free_tuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(PgDirectoryRelationId, HeapTupleGetOid(tuple), newOwnerId);
    }

    systable_endscan(scan);

    /* Close pg_database, but keep lock till commit */
    heap_close(rel, NoLock);
}
