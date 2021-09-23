/*-------------------------------------------------------------------------
 *
 * sharedfileset.cpp
 *	  Shared temporary file management.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/file/sharedfileset.cpp
 *
 * SharefFileSets provide a temporary namespace (think directory) so that
 * files can be discovered by name, and a shared ownership semantics so that
 * shared files survive until the last user detaches.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/hash.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "storage/sharedfileset.h"
#include "utils/builtins.h"

static void SharedFileSetPath(char *path, const SharedFileSet *fileset, Oid tablespace);
static void SharedFilePath(char *path, SharedFileSet *fileset, const char *name);
static Oid	ChooseTablespace(const SharedFileSet *fileset, const char *name);

/*
 * Initialize a space for temporary files that can be opened for read-only
 * access by other backends.  Other backends must attach to it before
 * accessing it.  Associate this SharedFileSet with 'seg'.  Any contained
 * files will be deleted when the last backend detaches.
 *
 * Files will be distributed over the tablespaces configured in
 * temp_tablespaces.
 *
 * Under the covers the set is one or more directories which will eventually
 * be deleted when there are no backends attached.
 */
void SharedFileSetInit(SharedFileSet *fileset)
{
    static THR_LOCAL uint32 counter = 0;

    SpinLockInit(&fileset->mutex);
    fileset->refcnt = 1;
    fileset->creator_pid = t_thrd.proc_cxt.MyProcPid;
    fileset->number = counter;
    counter = (counter + 1) % INT_MAX;

    /* Capture the tablespace OIDs so that all backends agree on them. */
    PrepareTempTablespaces();
    fileset->ntablespaces = GetTempTablespaces(&fileset->tablespaces[0], lengthof(fileset->tablespaces));
    if (fileset->ntablespaces == 0) {
        /* If the GUC is empty, use current database's default tablespace */
        fileset->tablespaces[0] = u_sess->proc_cxt.MyDatabaseTableSpace;
        fileset->ntablespaces = 1;
    } else {
        int i;

        /*
         * An entry of InvalidOid means use the default tablespace for the
         * current database.  Replace that now, to be sure that all users of
         * the SharedFileSet agree on what to do.
         */
        for (i = 0; i < fileset->ntablespaces; i++) {
            if (fileset->tablespaces[i] == InvalidOid) {
                fileset->tablespaces[i] = u_sess->proc_cxt.MyDatabaseTableSpace;
            }
        }
    }
}

/*
 * Attach to a set of directories that was created with SharedFileSetInit.
 */
void SharedFileSetAttach(SharedFileSet *fileset)
{
    bool success;

    SpinLockAcquire(&fileset->mutex);
    if (fileset->refcnt == 0)
        success = false;
    else {
        ++fileset->refcnt;
        success = true;
    }
    SpinLockRelease(&fileset->mutex);

    if (!success)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("could not attach to a SharedFileSet that is already destroyed")));
}

/*
 * Create a new file in the given set.
 */
File SharedFileSetCreate(SharedFileSet *fileset, const char *name)
{
    char path[MAXPGPATH];
    File file;

    SharedFilePath(path, fileset, name);
    file = PathNameCreateTemporaryFile(path, false);
    /* If we failed, see if we need to create the directory on demand. */
    if (file <= 0) {
        char tempdirpath[MAXPGPATH];
        char filesetpath[MAXPGPATH];
        Oid tablespace = ChooseTablespace(fileset, name);

        TempTablespacePath(tempdirpath, tablespace);
        SharedFileSetPath(filesetpath, fileset, tablespace);
        PathNameCreateTemporaryDir(tempdirpath, filesetpath);
        file = PathNameCreateTemporaryFile(path, true);
    }

    return file;
}

/*
 * Open a file that was created with SharedFileSetCreate(), possibly in
 * another backend.
 */
File SharedFileSetOpen(SharedFileSet *fileset, const char *name)
{
    char path[MAXPGPATH];
    File file;

    SharedFilePath(path, fileset, name);
    file = PathNameOpenTemporaryFile(path);

    return file;
}

/*
 * Delete a file that was created with PathNameCreateShared().
 * Return true if the file existed, false if didn't.
 */
bool SharedFileSetDelete(SharedFileSet *fileset, const char *name, bool errorOnFailure)
{
    char path[MAXPGPATH];

    SharedFilePath(path, fileset, name);

    return PathNameDeleteTemporaryFile(path, errorOnFailure);
}

/*
 * Delete all files in the set.
 */
void SharedFileSetDeleteAll(const SharedFileSet *fileset)
{
    char dirpath[MAXPGPATH];
    int i;

    /*
     * Delete the directory we created in each tablespace.  Doesn't fail
     * because we use this in error cleanup paths, but can generate LOG
     * message on IO error.
     */
    for (i = 0; i < fileset->ntablespaces; ++i) {
        SharedFileSetPath(dirpath, fileset, fileset->tablespaces[i]);
        PathNameDeleteTemporaryDir(dirpath);
    }
}

/*
 * Build the path for the directory holding the files backing a SharedFileSet
 * in a given tablespace.
 */
static void SharedFileSetPath(char *path, const SharedFileSet *fileset, Oid tablespace)
{
    char tempdirpath[MAXPGPATH];

    TempTablespacePath(tempdirpath, tablespace);
    errno_t rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s%lu.%u.sharedfileset",
        tempdirpath, PG_TEMP_FILE_PREFIX, (unsigned long)fileset->creator_pid, fileset->number);
    securec_check_ss(rc, "", "");
}

/*
 * Sorting hat to determine which tablespace a given shared temporary file
 * belongs in.
 */
static Oid ChooseTablespace(const SharedFileSet *fileset, const char *name)
{
    uint32 hash = hash_any((const unsigned char *)name, strlen(name));

    return fileset->tablespaces[hash % fileset->ntablespaces];
}

/*
 * Compute the full path of a file in a SharedFileSet.
 */
static void SharedFilePath(char *path, SharedFileSet *fileset, const char *name)
{
    char dirpath[MAXPGPATH];

    SharedFileSetPath(dirpath, fileset, ChooseTablespace(fileset, name));
    errno_t rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dirpath, name);
    securec_check_ss(rc, "", "");
}

