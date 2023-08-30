/* -------------------------------------------------------------------------
 *
 * relmapper.c
 *	  Catalog-to-filenode mapping
 *
 * For most tables, the physical file underlying the table is specified by
 * pg_class.relfilenode.  However, that obviously won't work for pg_class
 * itself, nor for the other "nailed" catalogs for which we have to be able
 * to set up working Relation entries without access to pg_class.  It also
 * does not work for shared catalogs, since there is no practical way to
 * update other databases' pg_class entries when relocating a shared catalog.
 * Therefore, for these special catalogs (henceforth referred to as "mapped
 * catalogs") we rely on a separately maintained file that shows the mapping
 * from catalog OIDs to filenode numbers.  Each database has a map file for
 * its local mapped catalogs, and there is a separate map file for shared
 * catalogs.  Mapped catalogs have zero in their pg_class.relfilenode entries.
 *
 * Relocation of a normal table is committed (ie, the new physical file becomes
 * authoritative) when the pg_class row update commits.  For mapped catalogs,
 * the act of updating the map file is effectively commit of the relocation.
 * We postpone the file update till just before commit of the transaction
 * doing the rewrite, but there is necessarily a window between.  Therefore
 * mapped catalogs can only be relocated by operations such as VACUUM FULL
 * and CLUSTER, which make no transactionally-significant changes: it must be
 * safe for the new file to replace the old, even if the transaction itself
 * aborts.	An important factor here is that the indexes and toast table of
 * a mapped catalog must also be mapped, so that the rewrites/relocations of
 * all these files commit in a single map file update rather than being tied
 * to transaction commit.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/relmapper.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/multi_redo_api.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/lock/lwlock.h"
#include "storage/file/fio_device.h"
#include "utils/inval.h"
#include "utils/relmapper.h"

/* non-export function prototypes */
static void apply_map_update(RelMapFile* map, Oid relationId, Oid fileNode, bool add_okay);
static void merge_map_updates(RelMapFile* map, const RelMapFile* updates, bool add_okay);
static void write_relmap_file(bool shared, RelMapFile* newmap, bool write_wal, bool send_sinval, bool preserve_files,
    Oid dbid, Oid tsid, const char* dbpath, XLogRecPtr redo_lsn = 0);
static void perform_relmap_update(bool shared, const RelMapFile* updates);
static int WriteOldVersionRelmap(RelMapFile* map, int fd);
static int ReadOldVersionRelmap(RelMapFile* map, int fd);
static void InitRelmapVerTag(RelMapVerTag* mapTag, bool isNewMap);
static bool IsNewVersionMap(int fd, char* fileName);
static void RegistRelMapWal(RelMapFile* map);
static pg_crc32 RelmapCrcComp(RelMapFile* map);
static int32 ReadRelMapFile(RelMapFile* map, int fd, bool isNewMap);
static int32 WriteRelMapFile(RelMapFile* map, int fd);

static void recover_relmap_file(bool shared, bool backupfile, RelMapFile* real_map);
/*
 * RelationMapOidToFilenode
 *
 * The raison d' etre ... given a relation OID, look up its filenode.
 *
 * Although shared and local relation OIDs should never overlap, the caller
 * always knows which we need --- so pass that information to avoid useless
 * searching.
 *
 * Returns InvalidOid if the OID is not known (which should never happen,
 * but the caller is in a better position to report a meaningful error).
 */
Oid RelationMapOidToFilenode(Oid relationId, bool shared)
{
    const RelMapFile* map = NULL;
    int32 i;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* If there are active updates, believe those over the main maps */
    if (shared) {
        map = relmap_cxt->active_shared_updates;
        for (i = 0; i < map->num_mappings; i++) {
            if (relationId == map->mappings[i].mapoid) {
                return map->mappings[i].mapfilenode;
            }
        }
        map = relmap_cxt->shared_map;
        for (i = 0; i < map->num_mappings; i++) {
            if (relationId == map->mappings[i].mapoid) {
                return map->mappings[i].mapfilenode;
            }
        }
    } else {
        map = relmap_cxt->active_local_updates;
        for (i = 0; i < map->num_mappings; i++) {
            if (relationId == map->mappings[i].mapoid) {
                return map->mappings[i].mapfilenode;
            }
        }
        map = relmap_cxt->local_map;
        for (i = 0; i < map->num_mappings; i++) {
            if (relationId == map->mappings[i].mapoid) {
                return map->mappings[i].mapfilenode;
            }
        }
    }

    return InvalidOid;
}

/*
 * RelationMapFilenodeToOid
 *
 * Do the reverse of the normal direction of mapping done in
 * RelationMapOidToFilenode.
 *
 * This is not supposed to be used during normal running but rather for
 * information purposes when looking at the filesystem or xlog.
 *
 * Returns InvalidOid if the OID is not known; this can easily happen if the
 * relfilenode doesn't pertain to a mapped relation.
 */
Oid RelationMapFilenodeToOid(Oid filenode, bool shared)
{
    const RelMapFile* map = NULL;
    int32 i;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* If there are active updates, believe those over the main maps */
    if (shared) {
        map = relmap_cxt->active_shared_updates;
        for (i = 0; i < map->num_mappings; i++) {
            if (filenode == map->mappings[i].mapfilenode) {
                return map->mappings[i].mapoid;
            }
        }
        map = relmap_cxt->shared_map;
        for (i = 0; i < map->num_mappings; i++) {
            if (filenode == map->mappings[i].mapfilenode) {
                return map->mappings[i].mapoid;
            }
        }
    } else {
        map = relmap_cxt->active_local_updates;
        for (i = 0; i < map->num_mappings; i++) {
            if (filenode == map->mappings[i].mapfilenode) {
                return map->mappings[i].mapoid;
            }
        }
        map = relmap_cxt->local_map;
        for (i = 0; i < map->num_mappings; i++) {
            if (filenode == map->mappings[i].mapfilenode) {
                return map->mappings[i].mapoid;
            }
        }
    }

    return InvalidOid;
}

/*
 * RelationMapUpdateMap
 *
 * Install a new relfilenode mapping for the specified relation.
 *
 * If immediate is true (or we're bootstrapping), the mapping is activated
 * immediately.  Otherwise it is made pending until CommandCounterIncrement.
 */
void RelationMapUpdateMap(Oid relationId, Oid fileNode, bool shared, bool immediate)
{
    RelMapFile* map = NULL;
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (IsBootstrapProcessingMode()) {
        /*
         * In bootstrap mode, the mapping gets installed in permanent map.
         */
        if (shared) {
            map = relmap_cxt->shared_map;
        } else {
            map = relmap_cxt->local_map;
        }
    } else {
        /*
         * We don't currently support map changes within subtransactions. This
         * could be done with more bookkeeping infrastructure, but it doesn't
         * presently seem worth it.
         */
        if (GetCurrentTransactionNestLevel() > 1) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot change relation mapping within subtransaction")));
        }
        if (immediate) {
            /* Make it active, but only locally */
            if (shared) {
                map = relmap_cxt->active_shared_updates;
            } else {
                map = relmap_cxt->active_local_updates;
            }
        } else {
            /* Make it pending */
            if (shared) {
                map = relmap_cxt->pending_shared_updates;
            } else {
                map = relmap_cxt->pending_local_updates;
            }
        }
    }
    apply_map_update(map, relationId, fileNode, true);
}

/*
 * apply_map_update
 *
 * Insert a new mapping into the given map variable, replacing any existing
 * mapping for the same relation.
 *
 * In some cases the caller knows there must be an existing mapping; pass
 * add_okay = false to draw an error if not.
 */
static void apply_map_update(RelMapFile* map, Oid relationId, Oid fileNode, bool add_okay)
{
    int32 i;
    RelMapVerTag mapTag;
    InitRelmapVerTag(&mapTag, IS_MAGIC_EXIST(map->magic) ? IS_NEW_RELMAP(map->magic) : true);

    /* Replace any existing mapping */
    for (i = 0; i < map->num_mappings; i++) {
        if (relationId == map->mappings[i].mapoid) {
            map->mappings[i].mapfilenode = fileNode;
            return;
        }
    }

    /* Nope, need to add a new mapping */
    if (!add_okay) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("attempt to apply a mapping to unmapped relation %u", relationId)));
    }

    if (map->num_mappings >= mapTag.relMaxMappings) {
        // switch relmap to new version
        if (map->magic == RELMAPPER_FILEMAGIC) {
            map->magic = RELMAPPER_FILEMAGIC_4K;
            ereport(LOG, (errmsg("Switch relmap to new version!")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("ran out of space in relation map")));
        }
    }
    map->mappings[map->num_mappings].mapoid = relationId;
    map->mappings[map->num_mappings].mapfilenode = fileNode;
    map->num_mappings++;
}

/*
 * merge_map_updates
 *
 * Merge all the updates in the given pending-update map into the target map.
 * This is just a bulk form of apply_map_update.
 */
static void merge_map_updates(RelMapFile* map, const RelMapFile* updates, bool add_okay)
{
    int32 i;

    for (i = 0; i < updates->num_mappings; i++) {
        apply_map_update(map, updates->mappings[i].mapoid, updates->mappings[i].mapfilenode, add_okay);
    }
}

/*
 * RelationMapRemoveMapping
 *
 * Remove a relation's entry in the map.  This is only allowed for "active"
 * (but not committed) local mappings.	We need it so we can back out the
 * entry for the transient target file when doing VACUUM FULL/CLUSTER on
 * a mapped relation.
 */
void RelationMapRemoveMapping(Oid relationId)
{
    RelMapFile* map = GetRelMapCxt()->active_local_updates;
    int32 i;

    for (i = 0; i < map->num_mappings; i++) {
        if (relationId == map->mappings[i].mapoid) {
            /* Found it, collapse it out */
            map->mappings[i] = map->mappings[map->num_mappings - 1];
            map->num_mappings--;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_NO_DATA_FOUND), errmsg("could not find temporary mapping for relation %u", relationId)));
}

/*
 * RelationMapInvalidate
 *
 * This routine is invoked for SI cache flush messages.  We must re-read
 * the indicated map file.	However, we might receive a SI message in a
 * process that hasn't yet, and might never, load the mapping files;
 * for example the autovacuum launcher, which *must not* try to read
 * a local map since it is attached to no particular database.
 * So, re-read only if the map is valid now.
 */
void RelationMapInvalidate(bool shared)
{
    if (EnableLocalSysCache()) {
        knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
        RelMapFile *rel_map = shared ? relmap_cxt->shared_map : relmap_cxt->local_map;
        if (IS_MAGIC_EXIST(rel_map->magic)) {
            t_thrd.lsc_cxt.lsc->LoadRelMapFromGlobal(shared);
        }
        return;
    }

    if (shared) {
        if (IS_MAGIC_EXIST(u_sess->relmap_cxt.shared_map->magic)) {
            LWLockAcquire(RelationMappingLock, LW_SHARED);
            load_relmap_file(true, u_sess->relmap_cxt.shared_map);
            LWLockRelease(RelationMappingLock);
        }
    } else {
        if (IS_MAGIC_EXIST(u_sess->relmap_cxt.local_map->magic)) {
            LWLockAcquire(RelationMappingLock, LW_SHARED);
            load_relmap_file(false, u_sess->relmap_cxt.local_map);
            LWLockRelease(RelationMappingLock);
        }
    }
}

/*
 * RelationMapInvalidateAll
 *
 * Reload all map files.  This is used to recover from SI message buffer
 * overflow: we can't be sure if we missed an inval message.
 * Again, reload only currently-valid maps.
 */
void RelationMapInvalidateAll(void)
{
    if (EnableLocalSysCache()) {
        knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
        if (IS_MAGIC_EXIST(relmap_cxt->shared_map->magic)) {
            t_thrd.lsc_cxt.lsc->LoadRelMapFromGlobal(true);
        }
        if (IS_MAGIC_EXIST(relmap_cxt->local_map->magic)) {
            t_thrd.lsc_cxt.lsc->LoadRelMapFromGlobal(false);
        }
        return;
    }
    
    LWLockAcquire(RelationMappingLock, LW_SHARED);
    if (IS_MAGIC_EXIST(u_sess->relmap_cxt.shared_map->magic)) {
        load_relmap_file(true, u_sess->relmap_cxt.shared_map);
    }
    if (IS_MAGIC_EXIST(u_sess->relmap_cxt.local_map->magic)) {
        load_relmap_file(false, u_sess->relmap_cxt.local_map);
    }
    LWLockRelease(RelationMappingLock);
}

/*
 * AtCCI_RelationMap
 *
 * Activate any "pending" relation map updates at CommandCounterIncrement time.
 */
void AtCCI_RelationMap(void)
{
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->pending_shared_updates->num_mappings != 0) {
        merge_map_updates(relmap_cxt->active_shared_updates, relmap_cxt->pending_shared_updates, true);
        relmap_cxt->pending_shared_updates->num_mappings = 0;
    }
    if (relmap_cxt->pending_local_updates->num_mappings != 0) {
        merge_map_updates(relmap_cxt->active_local_updates, relmap_cxt->pending_local_updates, true);
        relmap_cxt->pending_local_updates->num_mappings = 0;
    }
}

/*
 * AtEOXact_RelationMap
 *
 * Handle relation mapping at main-transaction commit or abort.
 *
 * During commit, this must be called as late as possible before the actual
 * transaction commit, so as to minimize the window where the transaction
 * could still roll back after committing map changes.	Although nothing
 * critically bad happens in such a case, we still would prefer that it
 * not happen, since we'd possibly be losing useful updates to the relations'
 * pg_class row(s).
 *
 * During abort, we just have to throw away any pending map changes.
 * Normal post-abort cleanup will take care of fixing relcache entries.
 */
void AtEOXact_RelationMap(bool isCommit)
{
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (isCommit) {
        /*
         * We should not get here with any "pending" updates.  (We could
         * logically choose to treat such as committed, but in the current
         * code this should never happen.)
         */
        Assert(relmap_cxt->pending_shared_updates->num_mappings == 0);
        Assert(relmap_cxt->pending_local_updates->num_mappings == 0);

        /*
         * Write any active updates to the actual map files, then reset them.
         */
        if (relmap_cxt->active_shared_updates->num_mappings != 0) {
            perform_relmap_update(true, relmap_cxt->active_shared_updates);
            relmap_cxt->active_shared_updates->num_mappings = 0;
        }
        if (relmap_cxt->active_local_updates->num_mappings != 0) {
            perform_relmap_update(false, relmap_cxt->active_local_updates);
            relmap_cxt->active_local_updates->num_mappings = 0;
        }
    } else {
        /* Abort --- drop all local and pending updates */
        relmap_cxt->active_shared_updates->num_mappings = 0;
        relmap_cxt->active_local_updates->num_mappings = 0;
        relmap_cxt->pending_shared_updates->num_mappings = 0;
        relmap_cxt->pending_local_updates->num_mappings = 0;
    }
}

/*
 * AtPrepare_RelationMap
 *
 * Handle relation mapping at PREPARE.
 *
 * Currently, we don't support preparing any transaction that changes the map.
 */
void AtPrepare_RelationMap(void)
{
    if (u_sess->attr.attr_common.IsInplaceUpgrade) {
        return;
    }
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    if (relmap_cxt->active_shared_updates->num_mappings != 0 ||
        relmap_cxt->active_local_updates->num_mappings != 0 ||
        relmap_cxt->pending_shared_updates->num_mappings != 0 ||
        relmap_cxt->pending_local_updates->num_mappings != 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot PREPARE a transaction that modified relation mapping")));
        }
}

/*
 * CheckPointRelationMap
 *
 * This is called during a checkpoint.	It must ensure that any relation map
 * updates that were WAL-logged before the start of the checkpoint are
 * securely flushed to disk and will not need to be replayed later.  This
 * seems unlikely to be a performance-critical issue, so we use a simple
 * method: we just take and release the RelationMappingLock.  This ensures
 * that any already-logged map update is complete, because write_relmap_file
 * will fsync the map file before the lock is released.
 */
void CheckPointRelationMap(void)
{
    LWLockAcquire(RelationMappingLock, LW_SHARED);
    LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapFinishBootstrap
 *
 * Write out the initial relation mapping files at the completion of
 * bootstrap.  All the mapped files should have been made known to us
 * via RelationMapUpdateMap calls.
 */
void RelationMapFinishBootstrap(void)
{
    Assert(IsBootstrapProcessingMode());

    if (ENABLE_DSS) {
        char map_file_name[MAXPGPATH];
        int rc = snprintf_s(map_file_name, sizeof(map_file_name), sizeof(map_file_name) - 1, "%s/global/%s",
            g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name, RELMAPPER_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");

        struct stat st;
        if (stat(map_file_name, &st) == 0 && S_ISREG(st.st_mode)) {
            return;
        }
    }

    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* Shouldn't be anything "pending" ... */
    Assert(relmap_cxt->active_shared_updates->num_mappings == 0);
    Assert(relmap_cxt->active_local_updates->num_mappings == 0);
    Assert(relmap_cxt->pending_shared_updates->num_mappings == 0);
    Assert(relmap_cxt->pending_local_updates->num_mappings == 0);

    /* Write the files; no WAL or sinval needed */
    write_relmap_file(true, relmap_cxt->shared_map, false, false, false, InvalidOid, GLOBALTABLESPACE_OID, NULL);
    write_relmap_file(false,
        relmap_cxt->local_map,
        false,
        false,
        false,
        u_sess->proc_cxt.MyDatabaseId,
        u_sess->proc_cxt.MyDatabaseTableSpace,
        u_sess->proc_cxt.DatabasePath);
}

/*
 * RelationMapInitialize
 *
 * This initializes the mapper module at process startup.  We can't access the
 * database yet, so just make sure the maps are empty.
 */
void RelationMapInitialize(void)
{
    if (EnableLocalSysCache()) {
        /* when first init or rebuild lsc, they are all palloc0, so nothing need to do
         * when switchdb , we memset them zero, nothing need to do */
        return;
    }
    /* The static variables should initialize to zeroes, but let's be sure */
    u_sess->relmap_cxt.shared_map->magic = 0; /* mark it not loaded */
    u_sess->relmap_cxt.local_map->magic = 0;
    u_sess->relmap_cxt.shared_map->num_mappings = 0;
    u_sess->relmap_cxt.local_map->num_mappings = 0;
    u_sess->relmap_cxt.active_shared_updates->num_mappings = 0;
    u_sess->relmap_cxt.active_local_updates->num_mappings = 0;
    u_sess->relmap_cxt.pending_shared_updates->num_mappings = 0;
    u_sess->relmap_cxt.pending_local_updates->num_mappings = 0;
}

/*
 * RelationMapInitializePhase2
 *
 * This is called to prepare for access to pg_database during startup.
 * We should be able to read the shared map file now.
 */
void RelationMapInitializePhase2(void)
{
    /*
     * In bootstrap mode, the map file isn't there yet, so do nothing.
     */
    if (IsBootstrapProcessingMode()) {
        return;
    }
    /*
     * Load the shared map file, die on error.
     */
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->InitRelMapPhase2();
        return;
    }
    LWLockAcquire(RelationMappingLock, LW_SHARED);
    load_relmap_file(true, u_sess->relmap_cxt.shared_map);
    LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapInitializePhase3
 *
 * This is called as soon as we have determined u_sess->proc_cxt.MyDatabaseId and set up
 * u_sess->proc_cxt.DatabasePath.  At this point we should be able to read the local map file.
 */
void RelationMapInitializePhase3(void)
{
    /*
     * In bootstrap mode, the map file isn't there yet, so do nothing.
     */
    if (IsBootstrapProcessingMode()) {
        return;
    }
    /*
     * Load the local map file, die on error.
     */
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->InitRelMapPhase3();
        return;
    }

    LWLockAcquire(RelationMappingLock, LW_SHARED);
    load_relmap_file(false, u_sess->relmap_cxt.local_map);
    LWLockRelease(RelationMappingLock);
}

/*
 * load_relmap_file -- load data from the shared or local map file
 *
 * Because the map file is essential for access to core system catalogs,
 * failure to read it is a fatal error.
 *
 * Note that the local case requires u_sess->proc_cxt.DatabasePath to be set up.
 */
void load_relmap_file(bool shared, RelMapFile *map)
{
    char map_file_name[2][MAXPGPATH];
    char* file_name = NULL;
    pg_crc32 crc;
    int fd;
    bool fix_backup = false;
    bool retry = false;
    struct stat stat_buf;
    errno_t rc;
    RelMapVerTag mapTag;
    bool isNewMap;

    if (shared) {
        rc = snprintf_s(map_file_name[0],
            sizeof(map_file_name[0]),
            sizeof(map_file_name[0]) - 1,
            "%s/%s",
            GLOTBSDIR,
            RELMAPPER_FILENAME);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(map_file_name[1],
            sizeof(map_file_name[1]),
            sizeof(map_file_name[1]) - 1,
            "%s/%s",
            GLOTBSDIR,
            RELMAPPER_FILENAME_BAK);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(map_file_name[0],
            sizeof(map_file_name[0]),
            sizeof(map_file_name[0]) - 1,
            "%s/%s",
            u_sess->proc_cxt.DatabasePath,
            RELMAPPER_FILENAME);
        securec_check_ss(rc, "\0", "\0");

        rc = snprintf_s(map_file_name[1],
            sizeof(map_file_name[1]),
            sizeof(map_file_name[1]) - 1,
            "%s/%s",
            u_sess->proc_cxt.DatabasePath,
            RELMAPPER_FILENAME_BAK);
        securec_check_ss(rc, "\0", "\0");
    }

    // check backup file
    if (stat(map_file_name[1], &stat_buf) != 0) {
        if (!FILE_POSSIBLY_DELETED(errno)) {
            ereport(LOG, (errmsg("can not stat file \"%s\", ignore backup file", map_file_name[1])));
        } else {
            fix_backup = true;
            // switch to exclusive lock to do backup map file recovery
            LWLockRelease(RelationMappingLock);
            LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
        }
    }

    file_name = map_file_name[0];
loop:

    /* Read data ... */
    fd = BasicOpenFile(file_name, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(
            FATAL, (errcode_for_file_access(), errmsg("could not open relation mapping file \"%s\": %m", file_name)));
    }
    /*
     * Note: we take RelationMappingLock in shared mode here, because it
     * seems that read() may not be atomic against any
     * concurrent updater's write().  If the file is updated shortly after we
     * look, the sinval signaling mechanism will make us re-read it before we
     * are able to access any relation that's affected by the change.
     */
    isNewMap = IsNewVersionMap(fd, file_name);
    (void)lseek(fd, 0, SEEK_SET);
    InitRelmapVerTag(&mapTag, isNewMap);
    int readBytes = ReadRelMapFile(map, fd, isNewMap);
    if (readBytes != mapTag.relSize) {
        close(fd);
        ereport(
            FATAL, (errcode_for_file_access(), errmsg("could not read relation mapping file \"%s\": %m", file_name)));
    }
    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }
    /* verify the CRC */
    crc = RelmapCrcComp(map);
    if (!EQ_CRC32(crc, (pg_crc32)(map->crc))) {
        if (retry == false) {
            ereport(WARNING,
                (errmsg("relation mapping file \"%s\" contains incorrect checksum, try backup file", file_name)));
            file_name = map_file_name[1];
            retry = true;

            if (fix_backup == false) {
                // switch to exclusive lock to do map file recovery
                LWLockRelease(RelationMappingLock);
                LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
            }
            goto loop;
        } else {
            ereport(FATAL, (errmsg("relation mapping file \"%s\" contains incorrect checksum", file_name)));
        }
    }

    // check for correct magic number, etc
    if (map->magic != mapTag.relMagic || map->num_mappings < 0 || map->num_mappings > mapTag.relMaxMappings) {
        ereport(FATAL, (errmsg("relation mapping file \"%s\" contains invalid data", file_name)));
    }
    if (retry == true) {
        recover_relmap_file(shared, false, map);
    } else if (fix_backup == true) {
        recover_relmap_file(shared, true, map);
    }

}

/*
 * Write out a new shared or local map file with the given contents.
 *
 * The magic number and CRC are automatically updated in *newmap.  On
 * success, we copy the data to the appropriate permanent static variable.
 *
 * If write_wal is TRUE then an appropriate WAL message is emitted.
 * (It will be false for bootstrap and WAL replay cases.)
 *
 * If send_sinval is TRUE then a SI invalidation message is sent.
 * (This should be true except in bootstrap case.)
 *
 * If preserve_files is TRUE then the storage manager is warned not to
 * delete the files listed in the map.
 *
 * Because this may be called during WAL replay when u_sess->proc_cxt.MyDatabaseId,
 * u_sess->proc_cxt.DatabasePath, etc aren't valid, we require the caller to pass in suitable
 * values.	The caller is also responsible for being sure no concurrent
 * map update could be happening.
 */
static void write_relmap_file(bool shared, RelMapFile* newmap, bool write_wal, bool send_sinval, bool preserve_files,
    Oid dbid, Oid tsid, const char* dbpath, XLogRecPtr redo_lsn)
{
    int fd;
    RelMapFile* real_map = NULL;
    char map_file_name[MAXPGPATH];
    errno_t rc = 0;
    char* fname[2];
    RelMapVerTag mapTag;

    if (!IS_MAGIC_EXIST(newmap->magic)) {
        newmap->magic = RELMAPPER_FILEMAGIC_4K;
    }
    InitRelmapVerTag(&mapTag, IS_NEW_RELMAP(newmap->magic));

    /*
     * Fill in the overhead fields and update CRC.
     */
    if (newmap->num_mappings < 0 || newmap->num_mappings > mapTag.relMaxMappings) {
        ereport(
            ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("attempt to write bogus relation mapping")));
    }
    newmap->crc = RelmapCrcComp(newmap);

    /*
     * Open the target file.  We prefer to do this before entering the
     * critical section, so that an open() failure need not force PANIC.
     *
     * Note: since we use BasicOpenFile, we are nominally responsible for
     * ensuring the fd is closed on error.	In practice, this isn't important
     * because either an error happens inside the critical section, or we are
     * in bootstrap or WAL replay; so an error past this point is always fatal
     * anyway.
     */
    fname[0] = RELMAPPER_FILENAME_BAK;
    fname[1] = RELMAPPER_FILENAME;

    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    for (int i = 0; i < 2; i++) {
        if (shared) {
            rc = snprintf_s(map_file_name,
                sizeof(map_file_name),
                sizeof(map_file_name) - 1,
                "%s/%s",
                GLOTBSDIR,
                fname[i]);
            securec_check_ss_c(rc, "\0", "\0");
            real_map = relmap_cxt->shared_map;
        } else {
            rc = snprintf_s(map_file_name,
                sizeof(map_file_name),
                sizeof(map_file_name) - 1,
                "%s/%s",
                dbpath,
                fname[i]);
            securec_check_ss_c(rc, "\0", "\0");
            real_map = relmap_cxt->local_map;
        }

        fd = BasicOpenFile(map_file_name, O_WRONLY | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open relation mapping file \"%s\": %m", map_file_name)));
        }
        if (write_wal && (i == 0)) {
            xl_relmap_update xlrec;
            XLogRecPtr lsn;

            /* now errors are fatal ... */
            START_CRIT_SECTION();

            xlrec.dbid = dbid;
            xlrec.tsid = tsid;
            if (IS_NEW_RELMAP(newmap->magic)) {
                xlrec.nbytes = sizeof(RelMapFile);
            } else {
                xlrec.nbytes = RELMAP_SIZE_OLD;
            }
            XLogBeginInsert();
            XLogRegisterData((char*)(&xlrec), MinSizeOfRelmapUpdate);
            RegistRelMapWal(newmap);
            lsn = XLogInsert(RM_RELMAP_ID, XLOG_RELMAP_UPDATE);

            /* As always, WAL must hit the disk before the data update does */
            XLogWaitFlush(lsn);
        }

        errno = 0;
        int writeBytes = WriteRelMapFile(newmap, fd);
        if (writeBytes != mapTag.relSize) {
            close(fd);
            /* if write didn't set errno, assume problem is no disk space */
            if (errno == 0) {
                errno = ENOSPC;
            }
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("could not write to relation mapping file \"%s\": %m", map_file_name)));
        }

        /*
         * We choose to fsync the data to disk before considering the task done.
         * It would be possible to relax this if it turns out to be a performance
         * issue, but it would complicate checkpointing --- see notes for
         * CheckPointRelationMap.
         */
        if (pg_fsync(fd) != 0) {
            close(fd);
            ereport(data_sync_elevel(ERROR),
                (errcode_for_file_access(), errmsg("could not fsync relation mapping file \"%s\": %m", map_file_name)));
        }

        if (close(fd)) {
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not close relation mapping file \"%s\": %m", map_file_name)));
        }
    }
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->InvalidateGlobalRelMap(shared, dbid, newmap);
    }
    /*
     * Now that the file is safely on disk, send sinval message to let other
     * backends know to re-read it.  We must do this inside the critical
     * section: if for some reason we fail to send the message, we have to
     * force a database-wide PANIC.  Otherwise other backends might continue
     * execution with stale mapping information, which would be catastrophic
     * as soon as others began to use the now-committed data.
     */
    if (send_sinval) {
        CacheInvalidateRelmap(dbid, redo_lsn);
    }
    /*
     * Make sure that the files listed in the map are not deleted if the outer
     * transaction aborts.	This had better be within the critical section
     * too: it's not likely to fail, but if it did, we'd arrive at transaction
     * abort with the files still vulnerable.  PANICing will leave things in a
     * good state on-disk.
     *
     * Note: we're cheating a little bit here by assuming that mapped files
     * are either in pg_global or the database's default tablespace.
     */
    if (preserve_files) {
        int32 i;

        for (i = 0; i < newmap->num_mappings; i++) {
            RelFileNode rnode;

            rnode.spcNode = tsid;
            rnode.dbNode = dbid;
            rnode.relNode = newmap->mappings[i].mapfilenode;
            RelationPreserveStorage(rnode, false);
        }
    }

    /* Success, update permanent copy */
    rc = memcpy_s(real_map, sizeof(RelMapFile), newmap, sizeof(RelMapFile));
    securec_check(rc, "", "");

    /* Critical section done */
    if (write_wal) {
        END_CRIT_SECTION();
    }
}

/*
 * Merge the specified updates into the appropriate "real" map,
 * and write out the changes.  This function must be used for committing
 * updates during normal multiuser operation.
 */
static void perform_relmap_update(bool shared, const RelMapFile* updates)
{
    RelMapFile new_map;
    errno_t rc;

    /*
     * Anyone updating a relation's mapping info should take exclusive lock on
     * that rel and hold it until commit.  This ensures that there will not be
     * concurrent updates on the same mapping value; but there could easily be
     * concurrent updates on different values in the same file. We cover that
     * by acquiring the RelationMappingLock, re-reading the target file to
     * ensure it's up to date, applying the updates, and writing the data
     * before releasing RelationMappingLock.
     *
     * There is only one RelationMappingLock.  In principle we could try to
     * have one per mapping file, but it seems unlikely to be worth the
     * trouble.
     */
    LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
    knl_u_relmap_context *relmap_cxt = GetRelMapCxt();
    /* Be certain we see any other updates just made */
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->LoadRelMapFromGlobal(shared);
    } else {
        load_relmap_file(shared, shared ? relmap_cxt->shared_map : relmap_cxt->local_map);
    }

    /* Prepare updated data in a local variable */
    if (shared) {
        rc = memcpy_s(&new_map, sizeof(RelMapFile), relmap_cxt->shared_map, sizeof(RelMapFile));
        securec_check(rc, "", "");
    } else {
        rc = memcpy_s(&new_map, sizeof(RelMapFile), relmap_cxt->local_map, sizeof(RelMapFile));
        securec_check(rc, "", "");
    }

    /*
     * Apply the updates to newmap.  No new mappings should appear, unless
     * somebody is adding indexes to system catalogs.
     */
    merge_map_updates(&new_map,
        updates,
        (g_instance.attr.attr_common.allowSystemTableMods || u_sess->attr.attr_common.IsInplaceUpgrade));

    Assert(CheckMyDatabaseMatch());
    /* Write out the updated map and do other necessary tasks */
    write_relmap_file(shared,
        &new_map,
        true,
        true,
        true,
        (shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId),
        (shared ? GLOBALTABLESPACE_OID : u_sess->proc_cxt.MyDatabaseTableSpace),
        u_sess->proc_cxt.DatabasePath);

    /* Now we can release the lock */
    LWLockRelease(RelationMappingLock);
}

/*
 * when incorrect checksum is detected in relation map file,
 * we should recover the file using the content of backup file or,
 * if there is no backup file, we create it immediately.
 */
static void recover_relmap_file(bool shared, bool backupfile, RelMapFile* real_map)
{
    int fd;
    char map_file_name[MAXPGPATH];
    char* file_name = NULL;
    int level;
    errno_t rc;
    int32 relmapSize;

    if (backupfile) {
        file_name = RELMAPPER_FILENAME_BAK;
        level = LOG;
    } else {
        file_name = RELMAPPER_FILENAME;
        level = WARNING;
    }

    if (shared) {
        rc = snprintf_s(map_file_name,
            sizeof(map_file_name),
            sizeof(map_file_name) - 1,
            "%s/%s",
            GLOTBSDIR,
            file_name);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(map_file_name,
            sizeof(map_file_name),
            sizeof(map_file_name) - 1,
            "%s/%s",
            u_sess->proc_cxt.DatabasePath,
            file_name);
        securec_check_ss(rc, "\0", "\0");
    }
    ereport(level, (errmsg("recover the relation mapping file %s", map_file_name)));

    fd = BasicOpenFile(map_file_name, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open relation mapping file \"%s\": %m",
            map_file_name)));
    }
    errno = 0;
    int writeBytes = 0;
    if (IS_NEW_RELMAP(real_map->magic)) {
        relmapSize = RELMAP_SIZE_NEW;
        writeBytes = write(fd, real_map, sizeof(RelMapFile));
    } else {
        relmapSize = RELMAP_SIZE_OLD;
        writeBytes = WriteOldVersionRelmap(real_map, fd);
    }
    if (writeBytes != relmapSize) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC,
            (errcode_for_file_access(),
                errmsg("recover failed could not write to relation mapping file \"%s\": %m", map_file_name)));
    }

    if (pg_fsync(fd) != 0) {
        ereport(PANIC,
            (errcode_for_file_access(),
                errmsg("recover failed could not fsync relation mapping file \"%s\": %m", map_file_name)));
    }
    if (close(fd)) {
        ereport(ERROR,
            (errcode_for_file_access(),
                errmsg("recover failed could not close relation mapping file \"%s\": %m", map_file_name)));
    }
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->InvalidateGlobalRelMap(shared,
            shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId, real_map);
    }
}

/*
 * RELMAP resource manager's routines
 */
void relmap_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /* Backup blocks are not used in relmap records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_RELMAP_UPDATE) {
        xl_relmap_update* xlrec = (xl_relmap_update*)XLogRecGetData(record);
        RelMapFile new_map;
        char* dbpath = NULL;
        errno_t rc;
        RelMapVerTag mapTag;
        int32 magicNum;

        rc = memcpy_s(&magicNum, sizeof(int32), xlrec->data, sizeof(int32));
        securec_check(rc, "\0", "\0");
        InitRelmapVerTag(&mapTag, IS_NEW_RELMAP(magicNum));

        if (xlrec->nbytes != mapTag.relSize) {
            ereport(PANIC,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("relmap_redo: wrong size %d in relmap update record", xlrec->nbytes)));
        }

        if (IS_NEW_RELMAP(magicNum)) {
            rc = memcpy_s(&new_map, sizeof(new_map), xlrec->data, sizeof(new_map));
            securec_check(rc, "\0", "\0");
        } else {
            rc = memcpy_s(&new_map, sizeof(new_map), xlrec->data, MAPPING_LEN_OLDMAP_HEAD);
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(&new_map.crc, MAPPING_LEN_TAIL, xlrec->data + MAPPING_LEN_OLDMAP_HEAD, MAPPING_LEN_TAIL);
            securec_check(rc, "\0", "\0");
        }
        /* We need to construct the pathname for this database */
        dbpath = GetDatabasePath(xlrec->dbid, xlrec->tsid);

        /*
         * Write out the new map and send sinval, but of course don't write a
         * new WAL entry.  There's no surrounding transaction to tell to
         * preserve files, either.
         *
         * There shouldn't be anyone else updating relmaps during WAL replay,
         * so we don't bother to take the RelationMappingLock.  We would need
         * to do so if load_relmap_file needed to interlock against writers.
         */
        XLogRecPtr lsn = record->EndRecPtr;
        UpdateMinRecoveryPoint(lsn, false);
        if (!IS_EXRTO_READ) {
            lsn = 0;
        }
        write_relmap_file((xlrec->dbid == InvalidOid), &new_map, false, true, false, xlrec->dbid, xlrec->tsid, dbpath,
            lsn);
        pfree_ext(dbpath);
    } else {
        ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("relmap_redo: unknown op code %u", info)));
    }
}

static int WriteOldVersionRelmap(RelMapFile* map, int fd)
{
    errno_t rc;
    char* mapCache_ori = NULL;
    char* mapCache = NULL;
    if (ENABLE_DSS) {
        mapCache_ori = (char*)palloc0(RELMAP_SIZE_OLD + ALIGNOF_BUFFER);
        mapCache = (char *)BUFFERALIGN(mapCache_ori);
    } else {
        mapCache = (char*)palloc0(RELMAP_SIZE_OLD);
    }
    rc = memcpy_s(mapCache, RELMAP_SIZE_OLD, map, MAPPING_LEN_OLDMAP_HEAD);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(
        mapCache + MAPPING_LEN_OLDMAP_HEAD, RELMAP_SIZE_OLD - MAPPING_LEN_OLDMAP_HEAD, &(map->crc), MAPPING_LEN_TAIL);
    securec_check(rc, "\0", "\0");
    int writeBytes = write(fd, mapCache, RELMAP_SIZE_OLD);
    if (ENABLE_DSS) {
        pfree_ext(mapCache_ori);
    } else {
        pfree_ext(mapCache);
    }
    return writeBytes;
}

static int ReadOldVersionRelmap(RelMapFile* map, int fd)
{
    errno_t rc;
    char* mapCache = (char*)palloc0(RELMAP_SIZE_OLD);
    int readByte = read(fd, mapCache, RELMAP_SIZE_OLD);
    rc = memcpy_s(map, sizeof(RelMapFile), mapCache, MAPPING_LEN_OLDMAP_HEAD);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(&(map->crc), MAPPING_LEN_TAIL, mapCache + MAPPING_LEN_OLDMAP_HEAD, MAPPING_LEN_TAIL);
    securec_check(rc, "\0", "\0");
    pfree_ext(mapCache);
    return readByte;
}

static void InitRelmapVerTag(RelMapVerTag* mapTag, bool isNewMap)
{
    if (isNewMap) {
        mapTag->relMagic = RELMAPPER_FILEMAGIC_4K;
        mapTag->relMaxMappings = MAX_MAPPINGS_4K;
        mapTag->relSize = RELMAP_SIZE_NEW;
    } else {
        mapTag->relMagic = RELMAPPER_FILEMAGIC;
        mapTag->relMaxMappings = MAX_MAPPINGS;
        mapTag->relSize = RELMAP_SIZE_OLD;
    }
}

static bool IsNewVersionMap(int fd, char* fileName)
{
    int32 magicNum;
    bool ret = false;
    int readByte = read(fd, &magicNum, sizeof(int32));
    if (readByte != sizeof(int32)) {
        close(fd);
        ereport(
            FATAL, (errcode_for_file_access(), errmsg("could not read relation mapping file \"%s\": %m", fileName)));
    }

    switch (magicNum) {
        case RELMAPPER_FILEMAGIC:
            ret = false;
            break;
        case RELMAPPER_FILEMAGIC_4K:
            ret = true;
            break;
        default:
            close(fd);
            ereport(FATAL,
                (errmsg("Illegal magic number %d for relation mapping file \"%s\": %m", magicNum, fileName)));
    }
    return ret;
}

static void RegistRelMapWal(RelMapFile* map)
{
    if (IS_NEW_RELMAP(map->magic)) {
        XLogRegisterData((char*)map, sizeof(RelMapFile));
    } else {
        XLogRegisterData((char*)map, MAPPING_LEN_OLDMAP_HEAD);
        XLogRegisterData((char*)&map->crc, MAPPING_LEN_TAIL);
    }
}

static pg_crc32 RelmapCrcComp(RelMapFile* map)
{
    pg_crc32 crc;
    INIT_CRC32(crc);
    if (IS_NEW_RELMAP(map->magic)) {
        COMP_CRC32(crc, (char*)map, offsetof(RelMapFile, crc));
    } else {
        COMP_CRC32(crc, (char*)map, MAPPING_LEN_OLDMAP_HEAD);
    }
    FIN_CRC32(crc);
    return crc;
}

static int32 ReadRelMapFile(RelMapFile* map, int fd, bool isNewMap)
{
    int32 readBytes = 0;
    if (isNewMap) {
        readBytes = read(fd, map, sizeof(RelMapFile));
    } else {
        readBytes = ReadOldVersionRelmap(map, fd);
    }
    return readBytes;
}

static int32 WriteRelMapFile(RelMapFile* map, int fd)
{
    int32 writeBytes = 0;
    char* unalignRelMap = NULL;
    RelMapFile* relMap = NULL;

    if (IS_NEW_RELMAP(map->magic)) {
        if (ENABLE_DSS) {
            unalignRelMap = (char*)palloc0(sizeof(RelMapFile) + ALIGNOF_BUFFER);
            relMap = (RelMapFile*)BUFFERALIGN(unalignRelMap);
            errno_t err = memcpy_s(relMap, sizeof(RelMapFile), map, sizeof(RelMapFile));
            securec_check(err, "\0", "\0");
        } else {
            relMap = map;
        }
        writeBytes = write(fd, relMap, sizeof(RelMapFile));
    } else {
        writeBytes = WriteOldVersionRelmap(map, fd);
    }

    if (unalignRelMap != NULL) {
        pfree(unalignRelMap);
    }
    return writeBytes;
}
