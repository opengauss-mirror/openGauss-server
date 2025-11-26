/* -------------------------------------------------------------------------
 *
 * reinit.cpp
 *	  Reinitialization of unlogged relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	      src/gausskernel/storage/file/reinit.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>

#include "catalog/catalog.h"
#include "storage/copydir.h"
#include "storage/smgr/fd.h"
#include "storage/reinit.h"
#include "storage/file/fio_device.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "ddes/dms/ss_transaction.h"
#ifdef PGXC
    #include "pgxc/pgxc.h"
#endif

static void ProcessUnloggedCache(const char *filename,
                                 const char *dbspacedirname,
                                 ForkNumber fork_num);
static void ResetUnloggedRelationsInTablespaceDir(const char* tsdirname, int op);
static void ResetUnloggedRelationsInDbspaceDir(const char* dbspacedirname, int op);
static bool parse_filename_for_nontemp_relation(const char* name, int* oidchars, ForkNumber* fork);

typedef struct {
    char oid[OIDCHARS + 1];
} unlogged_relation_entry;

/*
 * Reset unlogged relations from before the last restart.
 *
 * If op includes UNLOGGED_RELATION_CLEANUP, we remove all forks of any
 * relation with an "init" fork, except for the "init" fork itself.
 *
 * If op includes UNLOGGED_RELATION_INIT, we copy the "init" fork to the main
 * fork.
 */
void ResetUnloggedRelations(int op)
{
    char temp_path[MAXPGPATH];
    DIR* spc_dir = NULL;
    struct dirent* spc_de = NULL;
    MemoryContext tmpctx, oldctx;
    errno_t rc = EOK;

    /* Log it. */
    ereport(DEBUG1,
            (errmsg("resetting unlogged relations: cleanup %d init %d",
                    (op & UNLOGGED_RELATION_CLEANUP) != 0,
                    (op & UNLOGGED_RELATION_INIT) != 0)));

    /*
     * Just to be sure we don't leak any memory, let's create a temporary
     * memory context for this operation.
     */
    tmpctx = AllocSetContextCreate(CurrentMemoryContext,
                                   "ResetUnloggedRelations",
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldctx = MemoryContextSwitchTo(tmpctx);

    /*
     * First process unlogged files in pg_default ($PGDATA/base)
     */
    ResetUnloggedRelationsInTablespaceDir(DEFTBSDIR, op);
    /*
     * Cycle through directories for all non-default tablespaces.
     */
    spc_dir = AllocateDir(TBLSPCDIR);
    while ((spc_de = ReadDir(spc_dir, "pg_tblspc")) != NULL) {
        if (strcmp(spc_de->d_name, ".") == 0 || strcmp(spc_de->d_name, "..") == 0) {
            continue;
        }

#ifdef PGXC
        /* Postgres-XC tablespaces include the node name in path */
        if (ENABLE_DSS) {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY);
        } else {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s_%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName);
        }
#else
        rc = snprintf_s(temp_path,
                        sizeof(temp_path),
                        sizeof(temp_path) - 1,
                        "%s/%s/%s",
                        TBLSPCDIR,
                        spc_de->d_name,
                        TABLESPACE_VERSION_DIRECTORY);
#endif
        securec_check_ss(rc, "", "");
        ResetUnloggedRelationsInTablespaceDir(temp_path, op);
    }

    (void)FreeDir(spc_dir);

    /*
     * Restore memory context.
     */
    (void)MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(tmpctx);
}

/* Process unlogged relation file */
static void ProcessUnloggedCache(const char *filename,
                                 const char *dbspacedirname,
                                 ForkNumber fork_num)
{
    RelFileNodeBackend rnode_backend;
    Oid relfilenode = InvalidOid;
    Oid dbOid = InvalidOid;
    Oid spcOid = 0;

    /* extract refilenode */
    unsigned int tmpRel;
    if (sscanf_s(filename, "%u", &tmpRel) != 1) {
        ereport(DEBUG2, (errmsg("ResetUnloggedRelations: cannot parse relfilenode from '%s',"
            "skip buffer drop", filename)));
        return;
    }
    relfilenode = (Oid)tmpRel;
    /* extract dbOid and spcOid */
    unsigned int tmpOid;
    const char *last = strrchr(dbspacedirname, '/');
    if (last != NULL && *(last + 1) != '\0') {
        if (sscanf_s(last + 1, "%u", &tmpOid) == 1)
            dbOid = (Oid)tmpOid;
    }
    const char *prev = NULL;
    for (const char *q = last - 1; q >= dbspacedirname; q--) {
        if (*q == '/') {
            prev = q;
            break;
        }
    }
    if (prev != NULL && *(prev + 1) != '\0') {
        if (sscanf_s(prev + 1, "%u", &tmpOid) == 1)
            spcOid = (Oid)tmpOid;
    }

    rnode_backend.node.spcNode = spcOid;
    rnode_backend.node.dbNode  = OidIsValid(dbOid) ? dbOid : 0;
    rnode_backend.node.relNode = relfilenode;
    rnode_backend.node.bucketNode = InvalidBktId;
    rnode_backend.backend = InvalidBackendId;

    DropRelFileNodeBuffers(rnode_backend, fork_num, 0);
    ereport(DEBUG2, (errmsg(
        "ResetUnloggedRelations: dropped local buffers for rel %u/%u/%u",
        spcOid, dbOid, relfilenode)));

#ifdef ENABLE_DMS
    SSBCastDropRelAllBufferForUnlog(&rnode_backend.node, 1);
    ereport(DEBUG2, (errmsg(
        "ResetUnloggedRelations: broadcasted drop buffers for rel %u/%u/%u",
        spcOid, dbOid, relfilenode)));
#endif
}


/* Process one tablespace directory for ResetUnloggedRelations */
static void ResetUnloggedRelationsInTablespaceDir(const char* tsdirname, int op)
{
    DIR* ts_dir = NULL;
    struct dirent* de = NULL;
    char dbspace_path[MAXPGPATH];
    errno_t rc = EOK;

    ts_dir = AllocateDir(tsdirname);
    if (ts_dir == NULL) {
        /* anything except ENOENT is fishy */
        if (errno != ENOENT)
            ereport(LOG, (errmsg("could not open tablespace directory \"%s\": %m", tsdirname)));
        return;
    }

    while ((de = ReadDir(ts_dir, tsdirname)) != NULL) {
        int i = 0;

        /*
         * We're only interested in the per-database directories, which have
         * numeric names.  Note that this code will also (properly) ignore "."
         * and "..".
         */
        while (isdigit((unsigned char)de->d_name[i])) {
            ++i;
        }
        if (de->d_name[i] != '\0' || i == 0) {
            continue;
        }

        rc = snprintf_s(dbspace_path, sizeof(dbspace_path), sizeof(dbspace_path) - 1, "%s/%s", tsdirname, de->d_name);
        securec_check_ss(rc, "\0", "\0");

        ResetUnloggedRelationsInDbspaceDir(dbspace_path, op);
    }

    (void)FreeDir(ts_dir);
}

/* Process one per-dbspace directory for ResetUnloggedRelations */
static void ResetUnloggedRelationsInDbspaceDir(const char* dbspacedirname, int op)
{
    DIR* dbspace_dir = NULL;
    struct dirent* de = NULL;
    char rm_path[MAXPGPATH];

    /* Caller must specify at least one operation. */
    Assert((op & (UNLOGGED_RELATION_CLEANUP | UNLOGGED_RELATION_INIT)) != 0);

    /*
     * Cleanup is a two-pass operation.  First, we go through and identify all
     * the files with init forks.  Then, we go through again and nuke
     * everything with the same OID except the init fork.
     */
    if ((op & UNLOGGED_RELATION_CLEANUP) != 0) {
        HTAB* hash = NULL;
        HASHCTL ctl;

        /* Open the directory. */
        dbspace_dir = AllocateDir(dbspacedirname);
        if (dbspace_dir == NULL) {
            ereport(LOG, (errmsg("could not open dbspace directory \"%s\": %m", dbspacedirname)));
            return;
        }

        /*
         * It's possible that someone could create a ton of unlogged relations
         * in the same database & tablespace, so we'd better use a hash table
         * rather than an array or linked list to keep track of which files
         * need to be reset.  Otherwise, this cleanup operation would be
         * O(n^2).
         */
        ctl.keysize = sizeof(unlogged_relation_entry);
        ctl.entrysize = sizeof(unlogged_relation_entry);
        hash = hash_create("unlogged hash", 32, &ctl, HASH_ELEM);

        /* Scan the directory. */
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL) {
            ForkNumber fork_num;
            int oidchars;
            unlogged_relation_entry ent;
            errno_t rc;

            /* Skip anything that doesn't look like a relation data file. */
            if (!parse_filename_for_nontemp_relation(de->d_name, &oidchars, &fork_num)) {
                continue;
            }

            /* Also skip it unless this is the init fork. */
            if (fork_num != INIT_FORKNUM) {
                continue;
            }

            /*
             * Put the OID portion of the name into the hash table, if it
             * isn't already.
             */
            rc = memset_s(ent.oid, sizeof(ent.oid), 0, sizeof(ent.oid));
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(ent.oid, sizeof(ent.oid), de->d_name, oidchars);
            securec_check(rc, "\0", "\0");
            (void)hash_search(hash, &ent, HASH_ENTER, NULL);
        }

        /* Done with the first pass. */
        (void)FreeDir(dbspace_dir);

        /*
         * If we didn't find any init forks, there's no point in continuing;
         * we can bail out now.
         */
        if (hash_get_num_entries(hash) == 0) {
            hash_destroy(hash);
            return;
        }

        /*
         * Now, make a second pass and remove anything that matches. First,
         * reopen the directory.
         */
        dbspace_dir = AllocateDir(dbspacedirname);
        if (dbspace_dir == NULL) {
            ereport(LOG, (errmsg("could not open dbspace directory \"%s\": %m", dbspacedirname)));
            hash_destroy(hash);
            return;
        }

        /* Scan the directory. */
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL) {
            ForkNumber fork_num;
            int oidchars;
            bool found = false;
            errno_t rc;
            unlogged_relation_entry ent;

            /* Skip anything that doesn't look like a relation data file. */
            if (!parse_filename_for_nontemp_relation(de->d_name, &oidchars, &fork_num)) {
                continue;
            }

            /* We never remove the init fork. */
            if (fork_num == INIT_FORKNUM)
                continue;

            /*
             * See whether the OID portion of the name shows up in the hash
             * table.
             */
            rc = memset_s(ent.oid, sizeof(ent.oid), 0, sizeof(ent.oid));
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(ent.oid, sizeof(ent.oid), de->d_name, oidchars);
            securec_check(rc, "\0", "\0");

            (void)hash_search(hash, &ent, HASH_FIND, &found);

            /* If so, nuke it! */
            if (found) {
                rc = snprintf_s(rm_path, sizeof(rm_path), sizeof(rm_path) - 1, "%s/%s", dbspacedirname, de->d_name);

                securec_check_ss(rc, "\0", "\0");

                /* Process the unlogged relation dms cache */
                if (ENABLE_DMS) {
                    ProcessUnloggedCache(de->d_name, dbspacedirname, fork_num);
                }

                /*
                 * It's tempting to actually throw an error here, but since
                 * this code gets run during database startup, that could
                 * result in the database failing to start.  (XXX Should we do
                 * it anyway?)
                 */
                if (unlink(rm_path))
                    ereport(LOG, (errmsg("could not unlink file \"%s\": %m", rm_path)));
                else
                    ereport(DEBUG2, (errmsg("unlinked file \"%s\"", rm_path)));
            }
        }

        /* Cleanup is complete. */
        (void)FreeDir(dbspace_dir);
        hash_destroy(hash);
    }

    /*
     * Initialization happens after cleanup is complete: we copy each init
     * fork file to the corresponding main fork file.  Note that if we are
     * asked to do both cleanup and init, we may never get here: if the
     * cleanup code determines that there are no init forks in this dbspace,
     * it will return before we get to this point.
     */
    if ((op & UNLOGGED_RELATION_INIT) != 0) {
        /* Open the directory. */
        dbspace_dir = AllocateDir(dbspacedirname);
        if (dbspace_dir == NULL) {
            /* we just saw this directory, so it really ought to be there */
            ereport(LOG, (errmsg("could not open dbspace directory \"%s\": %m", dbspacedirname)));
            return;
        }

        /* Scan the directory. */
        while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL) {
            ForkNumber fork_num;
            int oidchars;
            char oidbuf[OIDCHARS + 1];
            char srcpath[MAXPGPATH];
            char dstpath[MAXPGPATH];
            errno_t rc;

            /* Skip anything that doesn't look like a relation data file. */
            if (!parse_filename_for_nontemp_relation(de->d_name, &oidchars, &fork_num))
                continue;

            /* Also skip it unless this is the init fork. */
            if (fork_num != INIT_FORKNUM)
                continue;

            /* Construct source pathname. */
            rc = snprintf_s(srcpath, sizeof(srcpath), sizeof(srcpath) - 1, "%s/%s", dbspacedirname, de->d_name);
            securec_check_ss(rc, "", "");
            /* Construct destination pathname. */
            rc = memcpy_s(oidbuf, sizeof(oidbuf), de->d_name, oidchars);
            securec_check(rc, "\0", "\0");

            oidbuf[oidchars] = '\0';
            rc = snprintf_s(dstpath,
                            sizeof(dstpath),
                            sizeof(dstpath) - 1,
                            "%s/%s%s",
                            dbspacedirname,
                            oidbuf,
                            de->d_name + oidchars + 1 + strlen(forkNames[INIT_FORKNUM]));
            securec_check_ss(rc, "", "");

            /* OK, we're ready to perform the actual copy. */
            ereport(DEBUG2, (errmsg("copying %s to %s", srcpath, dstpath)));
            copy_file(srcpath, dstpath);
        }

        /* Done with the first pass. */
        (void)FreeDir(dbspace_dir);
    }
}

/*
 * Basic parsing of putative relation filenames.
 *
 * This function returns true if the file appears to be in the correct format
 * for a non-temporary relation and false otherwise.
 *
 * NB: If this function returns true, the caller is entitled to assume that
 * *oidchars has been set to the a value no more than OIDCHARS, and thus
 * that a buffer of OIDCHARS+1 characters is sufficient to hold the OID
 * portion of the filename.  This is critical to protect against a possible
 * buffer overrun.
 */
static bool parse_filename_for_nontemp_relation(const char *name, int *oidchars, ForkNumber *fork)
{
    Assert(name != nullptr);
    size_t pos = 0;

    /* Look for a non-empty string of digits (that isn't too long). */
    for (pos = 0; isdigit((unsigned char)name[pos]); ++pos) {
        ;
    }
    if (pos == 0 || pos > OIDCHARS) {
        return false;
    }
    *oidchars = pos;

    /* Check for a fork name. */
    if (name[pos] != '_') {
        *fork = MAIN_FORKNUM;
    } else {
        int forkchar;

        forkchar = forkname_chars(&name[pos + 1], fork);
        if (forkchar <= 0) {
            return false;
        }
        pos += forkchar + 1;
    }

    /* Check for a segment number. */
    if (name[pos] == '.') {
        int segchar;

        for (segchar = 1; isdigit((unsigned char)name[pos + segchar]); ++segchar) {
            ;
        }
        if (segchar <= 1) {
            return false;
        }
        pos += segchar;
    }

    /* Now we should be at the end. */
    if (name[pos] != '\0') {
        return false;
    }
    return true;
}
