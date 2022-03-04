/* -------------------------------------------------------------------------
 *
 * origin.c
 * 	  Logical replication progress tracking support.
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 	  src/backend/replication/logical/origin.c
 *
 * NOTES
 *
 * This file provides the following:
 * * An infrastructure to name nodes in a replication setup
 * * A facility to efficiently store and persist replication progress in an
 * 	 efficient and durable manner.
 *
 * Replication origin consist out of a descriptive, user defined, external
 * name and a short, thus space efficient, internal 2 byte one. This split
 * exists because replication origin have to be stored in WAL and shared
 * memory and long descriptors would be inefficient.  For now only use 2 bytes
 * for the internal id of a replication origin as it seems unlikely that there
 * soon will be more than 65k nodes in one replication setup; and using only
 * two bytes allow us to be more space efficient.
 *
 * Replication progress is tracked in a shared memory table
 * (ReplicationState) that's dumped to disk every checkpoint. Entries
 * ('slots') in this table are identified by the internal id. That's the case
 * because it allows to increase replication progress during crash
 * recovery. To allow doing so we store the original LSN (from the originating
 * system) of a transaction in the commit record. That allows to recover the
 * precise replayed state after crash recovery; without requiring synchronous
 * commits. Allowing logical replication to use asynchronous commit is
 * generally good for performance, but especially important as it allows a
 * single threaded replay process to keep up with a source that has multiple
 * backends generating changes concurrently.  For efficiency and simplicity
 * reasons a backend can setup one replication origin that's from then used as
 * the source of changes produced by the backend, until reset again.
 *
 * This infrastructure is intended to be used in cooperation with logical
 * decoding. When replaying from a remote system the configured origin is
 * provided to output plugins, allowing prevention of replication loops and
 * other filtering.
 *
 * There are several levels of locking at work:
 *
 * * To create and drop replication origins an exclusive lock on
 * 	 pg_replication_slot is required for the duration. That allows us to
 * 	 safely and conflict free assign new origins using a dirty snapshot.
 *
 * * When creating an in-memory replication progress slot the ReplicationOrigin
 * 	 LWLock has to be held exclusively; when iterating over the replication
 * 	 progress a shared lock has to be held, the same when advancing the
 * 	 replication progress of an individual backend that has not setup as the
 * 	 session's replication origin.
 *
 * * When manipulating or looking at the remote_lsn and local_lsn fields of a
 * 	 replication progress slot that slot's lwlock has to be held. That's
 * 	 primarily because we do not assume 8 byte writes (the LSN) is atomic on
 * 	 all our platforms, but it also simplifies memory ordering concerns
 * 	 between the remote and local lsn. We use a lwlock instead of a spinlock
 * 	 so it's less harmful to hold the lock over a WAL write
 * 	 (cf. AdvanceReplicationProgress).
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/xact.h"

#include "catalog/catalog.h"
#include "catalog/indexing.h"

#include "nodes/execnodes.h"

#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/slot.h"
#include "replication/origin.h"

#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/copydir.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

static const int REPLICATION_ORIGIN_PROGRESS_COLS = 4;

/*
 * On disk version of ReplicationState.
 */
typedef struct ReplicationStateOnDisk {
    RepOriginId roident;
    XLogRecPtr remote_lsn;
} ReplicationStateOnDisk;

/* Magic for on disk files. */
#define REPLICATION_STATE_MAGIC ((uint32)0x1257DADE)

static void replorigin_check_prerequisites(bool check_slots, bool recoveryOK)
{
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("openGauss does not support replication origin yet"),
                errdetail("The feature is not currently supported")));
#endif
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before GRAND VERSION NUM %u, we do not support replication origin.", PUBLICATION_VERSION_NUM)));
    }

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("only superusers can query or manipulate replication origins")));
    }

    if (check_slots && g_instance.attr.attr_storage.max_replication_slots == 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("cannot query or manipulate replication origin when max_replication_slots = 0")));
    }

    if (!recoveryOK && RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot manipulate replication origins during recovery")));
    }
}


/* ---------------------------------------------------------------------------
 * Functions for working with replication origins themselves.
 * ---------------------------------------------------------------------------
 */

/*
 * Check for a persistent replication origin identified by name.
 *
 * Returns InvalidOid if the node isn't known yet and missing_ok is true.
 */
RepOriginId replorigin_by_name(const char *roname, bool missing_ok)
{
    Form_pg_replication_origin ident;
    Oid roident = InvalidOid;
    HeapTuple tuple;
    ScanKeyData key;

    Relation rel = heap_open(ReplicationOriginRelationId, AccessShareLock);

    ScanKeyInit(&key, Anum_pg_replication_origin_roname, BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(roname));

    SysScanDesc scan = systable_beginscan(rel, ReplicationOriginNameIndex, true /* indexOK */, SnapshotNow, 1, &key);

    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        ident = (Form_pg_replication_origin)GETSTRUCT(tuple);
        roident = ident->roident;
    } else if (!missing_ok) {
        systable_endscan(scan);
        heap_close(rel, AccessShareLock);
        elog(ERROR, "could not find tuple for replication origin '%s'", roname);
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
    return roident;
}

/*
 * Create a replication origin.
 *
 * Needs to be called in a transaction.
 */
RepOriginId replorigin_create(const char *roname)
{
    Oid roident;
    HeapTuple tuple = NULL;
    Relation rel;
    Datum roname_d;
    SnapshotData SnapshotDirty;
    SysScanDesc scan;
    ScanKeyData key;
    int rc;

    roname_d = CStringGetTextDatum(roname);

    Assert(IsTransactionState());

    /*
     * We need the numeric replication origin to be 16bit wide, so we cannot
     * rely on the normal oid allocation. Instead we simply scan
     * pg_replication_origin for the first unused id. That's not particularly
     * efficient, but this should be a fairly infrequent operation - we can
     * easily spend a bit more code on this when it turns out it needs to be
     * faster.
     *
     * We handle concurrency by taking an exclusive lock (allowing reads!)
     * over the table for the duration of the search. Because we use a "dirty
     * snapshot" we can read rows that other in-progress sessions have
     * written, even though they would be invisible with normal snapshots. Due
     * to the exclusive lock there's no danger that new rows can appear while
     * we're checking.
     */
    InitDirtySnapshot(SnapshotDirty);

    rel = heap_open(ReplicationOriginRelationId, ExclusiveLock);

    for (roident = InvalidOid + 1; roident < PG_UINT16_MAX; roident++) {
        bool nulls[Natts_pg_replication_origin];
        Datum values[Natts_pg_replication_origin];
        bool collides;

        CHECK_FOR_INTERRUPTS();

        ScanKeyInit(&key, Anum_pg_replication_origin_roident, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(roident));

        scan = systable_beginscan(rel, ReplicationOriginIdentIndex, true /* indexOK */, &SnapshotDirty, 1, &key);

        collides = HeapTupleIsValid(systable_getnext(scan));

        systable_endscan(scan);

        if (!collides) {
            /*
             * Ok, found an unused roident, insert the new row and do a CCI,
             * so our callers can look it up if they want to.
             */
            rc = memset_s(&nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(rc, "", "");

            values[Anum_pg_replication_origin_roident - 1] = ObjectIdGetDatum(roident);
            values[Anum_pg_replication_origin_roname - 1] = roname_d;

            tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
            simple_heap_insert(rel, tuple);
            CatalogUpdateIndexes(rel, tuple);
            CommandCounterIncrement();
            break;
        }
    }

    /* now release lock again,	*/
    heap_close(rel, ExclusiveLock);

    if (tuple == NULL)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("could not find free replication origin OID")));

    heap_freetuple(tuple);
    return roident;
}


/*
 * Helper function to drop a replication origin.
 */
static void replorigin_drop_guts(Relation rel, RepOriginId roident)
{
    HeapTuple tuple = NULL;
    ScanKeyData key;
    int i;

    /* cleanup the slot state info */
    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationState *state = &u_sess->reporigin_cxt.repStatesShm->states[i];

        /* found our slot */
        if (state->roident == roident) {
            if (state->acquired_by != 0) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                    errmsg("could not drop replication origin with OID %d, in use by PID %lu", state->roident,
                    state->acquired_by)));
            }

            /* first WAL log */
            {
                xl_replorigin_drop xlrec;

                xlrec.node_id = roident;
                XLogBeginInsert();
                XLogRegisterData((char *)(&xlrec), sizeof(xlrec));
                XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_DROP);
            }

            /* then reset the in-memory entry */
            state->roident = InvalidRepOriginId;
            state->remote_lsn = InvalidXLogRecPtr;
            state->local_lsn = InvalidXLogRecPtr;
            break;
        }
    }
    LWLockRelease(ReplicationOriginLock);

    ScanKeyInit(&key, Anum_pg_replication_origin_roident, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roident));

    SysScanDesc scan = systable_beginscan(rel, ReplicationOriginIdentIndex, true /* indexOK */, SnapshotNow, 1, &key);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        elog(ERROR, "could not find tuple for replication origin with oid %u", roident);
    }

    simple_heap_delete(rel, &tuple->t_self);

    CommandCounterIncrement();
    systable_endscan(scan);
}

/*
 * Drop replication origin (by name).
 *
 * Needs to be called in a transaction.
 */
void replorigin_drop_by_name(const char *name, bool missing_ok)
{
    RepOriginId roident;
    Relation rel;

    Assert(IsTransactionState());

    /*
     * To interlock against concurrent drops, we hold ExclusiveLock on
     * pg_replication_origin till xact commit.
     *
     * XXX We can optimize this by acquiring the lock on a specific origin by
     * using LockSharedObject if required. However, for that, we first to
     * acquire a lock on ReplicationOriginRelationId, get the origin_id, lock
     * the specific origin and then re-check if the origin still exists.
     */
    rel = heap_open(ReplicationOriginRelationId, ExclusiveLock);

    roident = replorigin_by_name(name, missing_ok);
    if (OidIsValid(roident)) {
        replorigin_drop_guts(rel, roident);
    }

    /* We keep the lock on pg_replication_origin until commit */
    heap_close(rel, NoLock);
}


/*
 * Lookup replication origin via it's oid and return the name.
 *
 * The external name is palloc'd in the calling context.
 *
 * Returns true if the origin is known, false otherwise.
 */
bool replorigin_by_oid(RepOriginId roident, bool missing_ok, char **roname)
{
    HeapTuple tuple;
    Form_pg_replication_origin ric;

    Assert(OidIsValid((Oid)roident));
    Assert(roident != InvalidRepOriginId);
    Assert(roident != DoNotReplicateId);

    Relation rel = heap_open(ReplicationOriginRelationId, AccessShareLock);
    ScanKeyData key;

    ScanKeyInit(&key, Anum_pg_replication_origin_roident, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum((Oid)roident));

    SysScanDesc scan = systable_beginscan(rel, ReplicationOriginIdentIndex, true /* indexOK */, SnapshotNow, 1, &key);

    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        ric = (Form_pg_replication_origin)GETSTRUCT(tuple);
        *roname = text_to_cstring(&ric->roname);
        systable_endscan(scan);
        heap_close(rel, AccessShareLock);

        return true;
    } else {
        *roname = NULL;
        systable_endscan(scan);
        heap_close(rel, AccessShareLock);

        if (!missing_ok)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("replication origin with OID %u does not exist", roident)));

        return false;
    }
}


/* ---------------------------------------------------------------------------
 * Functions for handling replication progress.
 * ---------------------------------------------------------------------------
 */

Size ReplicationOriginShmemSize(void)
{
    Size size = 0;

    /*
     * XXX: max_replication_slots is arguably the wrong thing to use, as here
     * we keep the replay state of *remote* transactions. But for now it seems
     * sufficient to reuse it, lest we introduce a separate guc.
     */
    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return size;

    size = add_size(size, offsetof(ReplicationStateShmStruct, states));

    size = add_size(size, mul_size(g_instance.attr.attr_storage.max_replication_slots, sizeof(ReplicationState)));
    return size;
}

void ReplicationOriginShmemInit(void)
{
    bool found;
    int rc;
    Size size = ReplicationOriginShmemSize();
    if (size == 0) {
        return;
    }

    u_sess->reporigin_cxt.repStatesShm =
        (ReplicationStateShmStruct *)ShmemInitStruct("ReplicationOriginState", size, &found);

    if (!found) {
        int i;

        rc = memset_s(u_sess->reporigin_cxt.repStatesShm, size, 0, size);
        securec_check(rc, "", "");
        u_sess->reporigin_cxt.repStatesShm->tranche_id = LWTRANCHE_REPLICATION_ORIGIN;

        for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            LWLockInitialize(&u_sess->reporigin_cxt.repStatesShm->states[i].lock,
                u_sess->reporigin_cxt.repStatesShm->tranche_id);
        }
    }
}

/* ---------------------------------------------------------------------------
 * Perform a checkpoint of each replication origin's progress with respect to
 * the replayed remote_lsn. Make sure that all transactions we refer to in the
 * checkpoint (local_lsn) are actually on-disk. This might not yet be the case
 * if the transactions were originally committed asynchronously.
 *
 * We store checkpoints in the following format:
 * +-------+------------------------+------------------+-----+--------+
 * | MAGIC | ReplicationStateOnDisk | struct Replic... | ... | CRC32C | EOF
 * +-------+------------------------+------------------+-----+--------+
 *
 * So its just the magic, followed by the statically sized
 * ReplicationStateOnDisk structs. Note that the maximum number of
 * ReplicationState is determined by max_replication_slots.
 * ---------------------------------------------------------------------------
 */
void CheckPointReplicationOrigin(void)
{
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(LOG,
            (errmsg("Before GRAND VERSION NUM %u, we do not support replication origin.", PUBLICATION_VERSION_NUM)));
        return;
    }
    const char *tmppath = "pg_logical/replorigin_checkpoint.tmp";
    const char *path = "pg_logical/replorigin_checkpoint";
    int tmpfd;
    int i;
    int rc;
    uint32 magic = REPLICATION_STATE_MAGIC;
    pg_crc32c crc;
    struct stat st;

    if (g_instance.attr.attr_storage.max_replication_slots == 0) {
        return;
    }

    INIT_CRC32C(crc);

    /* make sure no old temp file is remaining */
    if (unlink(tmppath) < 0 && errno != ENOENT) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", tmppath)));
    }

    if (unlikely(stat("pg_logical", &st) != 0)) {
        /* maybe other threads have created this directory, so check the errno again */
        if ((mkdir("pg_logical", S_IRWXU) != 0) && (errno != EEXIST)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not create directory \"pg_logical\": %m")));
        }
    }

    /*
     * no other backend can perform this at the same time; only one checkpoint
     * can happen at a time.
     */
    tmpfd = OpenTransientFile((char *)tmppath, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (tmpfd < 0)
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", tmppath)));

    /* write magic */
    errno = 0;
    if ((write(tmpfd, &magic, sizeof(magic))) != sizeof(magic)) {
        CloseTransientFile(tmpfd);
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
    }
    COMP_CRC32C(crc, &magic, sizeof(magic));

    /* prevent concurrent creations/drops */
    LWLockAcquire(ReplicationOriginLock, LW_SHARED);

    /* write actual data */
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationStateOnDisk disk_state;
        ReplicationState *curstate = &u_sess->reporigin_cxt.repStatesShm->states[i];
        XLogRecPtr local_lsn;

        if (curstate->roident == InvalidRepOriginId) {
            continue;
        }

        /* zero, to avoid uninitialized padding bytes */
        rc = memset_s(&disk_state, sizeof(disk_state), 0, sizeof(disk_state));
        securec_check(rc, "", "");

        LWLockAcquire(&curstate->lock, LW_SHARED);

        disk_state.roident = curstate->roident;

        disk_state.remote_lsn = curstate->remote_lsn;
        local_lsn = curstate->local_lsn;

        LWLockRelease(&curstate->lock);

        /* make sure we only write out a commit that's persistent */
        XLogWaitFlush(local_lsn);

        errno = 0;
        if ((write(tmpfd, &disk_state, sizeof(disk_state))) != sizeof(disk_state)) {
            CloseTransientFile(tmpfd);
            /* if write didn't set errno, assume problem is no disk space */
            if (errno == 0) {
                errno = ENOSPC;
            }
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
        }

        COMP_CRC32C(crc, &disk_state, sizeof(disk_state));
    }

    LWLockRelease(ReplicationOriginLock);

    /* write out the CRC */
    FIN_CRC32C(crc);
    errno = 0;
    if ((write(tmpfd, &crc, sizeof(crc))) != sizeof(crc)) {
        CloseTransientFile(tmpfd);
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
    }

    if (CloseTransientFile(tmpfd) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
    }

    /* fsync, rename to permanent file, fsync file and directory */
    durable_rename(tmppath, path, PANIC);
}

/*
 * Recover replication replay status from checkpoint data saved earlier by
 * CheckPointReplicationOrigin.
 *
 * This only needs to be called at startup and *not* during every checkpoint
 * read during recovery (e.g. in HS or PITR from a base backup) afterwards. All
 * state thereafter can be recovered by looking at commit records.
 */
void StartupReplicationOrigin(void)
{
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(LOG,
            (errmsg("Before GRAND VERSION NUM %u, we do not support replication origin.", PUBLICATION_VERSION_NUM)));
        return;
    }
    const char *path = "pg_logical/replorigin_checkpoint";
    int fd;
    int readBytes;
    uint32 magic = REPLICATION_STATE_MAGIC;
    int last_state = 0;
    pg_crc32c file_crc;
    pg_crc32c crc;

    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        return;

    INIT_CRC32C(crc);

    elog(DEBUG2, "starting up replication origin progress state");

    fd = OpenTransientFile((char *)path, O_RDONLY | PG_BINARY, 0);
    /*
     * might have had max_replication_slots == 0 last run, or we just brought
     * up a standby.
     */
    if (fd < 0 && errno == ENOENT) {
        return;
    } else if (fd < 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
    }

    /* verify magic, that is written even if nothing was active */
    readBytes = read(fd, &magic, sizeof(magic));
    if (readBytes != sizeof(magic)) {
        ereport(PANIC, (errmsg("could not read file \"%s\": %m", path)));
    }
    COMP_CRC32C(crc, &magic, sizeof(magic));

    if (magic != REPLICATION_STATE_MAGIC) {
        ereport(PANIC,
            (errmsg("replication checkpoint has wrong magic %u instead of %u", magic, REPLICATION_STATE_MAGIC)));
    }

    /* we can skip locking here, no other access is possible */

    /* recover individual states, until there are no more to be found */
    while (true) {
        ReplicationStateOnDisk disk_state;

        readBytes = read(fd, &disk_state, sizeof(disk_state));
        /* no further data */
        if (readBytes == sizeof(crc)) {
            /* not pretty, but simple ... */
            file_crc = *(pg_crc32c *)&disk_state;
            break;
        }

        if (readBytes < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", path)));
        }

        if (readBytes != sizeof(disk_state)) {
            ereport(PANIC, (errcode_for_file_access(),
                errmsg("could not read file \"%s\": read %d of %zu", path, readBytes, sizeof(disk_state))));
        }

        COMP_CRC32C(crc, &disk_state, sizeof(disk_state));

        if (last_state == g_instance.attr.attr_storage.max_replication_slots) {
            ereport(PANIC, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                errmsg("could not find free replication state, increase max_replication_slots")));
        }

        /* copy data to shared memory */
        u_sess->reporigin_cxt.repStatesShm->states[last_state].roident = disk_state.roident;
        if (u_sess->reporigin_cxt.repStatesShm->states[last_state].remote_lsn > disk_state.remote_lsn) {
            /*
             * This may happen in standby when doing switchover. before calling startup, standby has
             * already redo a xact_commit, which set a lastest remote_lsn to shared memory(check
             * xact_redo_commit_internal -> replorigin_advance). In this case, we can just ignore the
             * value in disk.
             */
            XLogRecPtr currentLsn = u_sess->reporigin_cxt.repStatesShm->states[last_state].remote_lsn;
            ereport(LOG, (errmsg("try to recover a older replication state from disk, ignore it. "
                "current remote_lsn: %X/%X, remote_lsn in disk: %X/%X",
                (uint32)(currentLsn >> BITS_PER_INT), (uint32)currentLsn,
                (uint32)(disk_state.remote_lsn >> BITS_PER_INT), (uint32)disk_state.remote_lsn)));
        } else {
            u_sess->reporigin_cxt.repStatesShm->states[last_state].remote_lsn = disk_state.remote_lsn;
        }
        last_state++;

        elog(LOG, "recovered replication state of node %u to %X/%X", disk_state.roident,
            (uint32)(disk_state.remote_lsn >> BITS_PER_INT), (uint32)disk_state.remote_lsn);
    }

    /* now check checksum */
    FIN_CRC32C(crc);
    if (file_crc != crc) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("replication slot checkpoint has wrong checksum %u, expected %u", crc, file_crc)));
    }

    if (CloseTransientFile(fd) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
    }
}

void replorigin_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_REPLORIGIN_SET: {
            xl_replorigin_set *xlrec = (xl_replorigin_set *)XLogRecGetData(record);

            replorigin_advance(xlrec->node_id, xlrec->remote_lsn, record->EndRecPtr, xlrec->force /* backward */,
                false /* WAL log */);
            break;
        }
        case XLOG_REPLORIGIN_DROP: {
            xl_replorigin_drop *xlrec;
            int i;

            xlrec = (xl_replorigin_drop *)XLogRecGetData(record);

            for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                ReplicationState *state = &u_sess->reporigin_cxt.repStatesShm->states[i];

                /* found our slot */
                if (state->roident == xlrec->node_id) {
                    /* reset entry */
                    state->roident = InvalidRepOriginId;
                    state->remote_lsn = InvalidXLogRecPtr;
                    state->local_lsn = InvalidXLogRecPtr;
                    break;
                }
            }
            break;
        }
        default:
            elog(PANIC, "replorigin_redo: unknown op code %u", info);
    }
}

const char* replorigin_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_REPLORIGIN_SET) {
        return "set_replication_origin";
    } else if (info == XLOG_REPLORIGIN_DROP) {
        return "drop_replication_origin";
    } else {
        return "unkown_type";
    }
}

/*
 * Tell the replication origin progress machinery that a commit from 'node'
 * that originated at the LSN remote_commit on the remote node was replayed
 * successfully and that we don't need to do so again. In combination with
 * setting up t_thrd.reporigin_cxt.originLsn and t_thrd.reporigin_cxt.originId
 * that ensures we won't lose knowledge about that after a crash if the
 * transaction had a persistent effect (think of asynchronous commits).
 *
 * local_commit needs to be a local LSN of the commit so that we can make sure
 * upon a checkpoint that enough WAL has been persisted to disk.
 *
 * Needs to be called with a RowExclusiveLock on pg_replication_origin,
 * unless running in recovery.
 */
void replorigin_advance(RepOriginId node, XLogRecPtr remote_commit, XLogRecPtr local_commit, bool go_backward,
    bool wal_log)
{
    int i;
    ReplicationState *replication_state = NULL;
    ReplicationState *free_state = NULL;

    Assert(node != InvalidRepOriginId);

    /* we don't track DoNotReplicateId */
    if (node == DoNotReplicateId)
        return;

    /*
     * XXX: For the case where this is called by WAL replay, it'd be more
     * efficient to restore into a backend local hashtable and only dump into
     * shmem after recovery is finished. Let's wait with implementing that
     * till it's shown to be a measurable expense
     */

    /* Lock exclusively, as we may have to create a new table entry. */
    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    /*
     * Search for either an existing slot for the origin, or a free one we can
     * use.
     */
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationState *curstate = &u_sess->reporigin_cxt.repStatesShm->states[i];

        /* remember where to insert if necessary */
        if (curstate->roident == InvalidRepOriginId && free_state == NULL) {
            free_state = curstate;
            continue;
        }

        /* not our slot */
        if (curstate->roident != node) {
            continue;
        }

        /* ok, found slot */
        replication_state = curstate;

        LWLockAcquire(&replication_state->lock, LW_EXCLUSIVE);

        /* Make sure it's not used by somebody else */
        if (replication_state->acquired_by != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE), errmsg("replication origin with OID %d is already active for PID %lu",
                replication_state->roident, replication_state->acquired_by)));
        }

        break;
    }

    if (replication_state == NULL && free_state == NULL)
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            errmsg("could not find free replication state slot for replication origin with OID %u", node),
            errhint("Increase max_replication_slots and try again.")));

    if (replication_state == NULL) {
        /* initialize new slot */
        LWLockAcquire(&free_state->lock, LW_EXCLUSIVE);
        replication_state = free_state;
        Assert(replication_state->remote_lsn == InvalidXLogRecPtr);
        Assert(replication_state->local_lsn == InvalidXLogRecPtr);
        replication_state->roident = node;
    }

    Assert(replication_state->roident != InvalidRepOriginId);

    /*
     * If somebody "forcefully" sets this slot, WAL log it, so it's durable
     * and the standby gets the message. Primarily this will be called during
     * WAL replay (of commit records) where no WAL logging is necessary.
     */
    if (wal_log) {
        xl_replorigin_set xlrec;

        xlrec.remote_lsn = remote_commit;
        xlrec.node_id = node;
        xlrec.force = go_backward;

        XLogBeginInsert();
        XLogRegisterData((char *)(&xlrec), sizeof(xlrec));

        XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET);
    }

    /*
     * Due to - harmless - race conditions during a checkpoint we could see
     * values here that are older than the ones we already have in memory.
     * Don't overwrite those.
     */
    if (go_backward || replication_state->remote_lsn < remote_commit)
        replication_state->remote_lsn = remote_commit;
    if (local_commit != InvalidXLogRecPtr && (go_backward || replication_state->local_lsn < local_commit))
        replication_state->local_lsn = local_commit;
    LWLockRelease(&replication_state->lock);

    /*
     * Release *after* changing the LSNs, slot isn't acquired and thus could
     * otherwise be dropped anytime.
     */
    LWLockRelease(ReplicationOriginLock);
}


static XLogRecPtr replorigin_get_progress(RepOriginId node, bool flush)
{
    int i;
    XLogRecPtr local_lsn = InvalidXLogRecPtr;
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;

    /* prevent slots from being concurrently dropped */
    LWLockAcquire(ReplicationOriginLock, LW_SHARED);

    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationState *state;

        state = &u_sess->reporigin_cxt.repStatesShm->states[i];

        if (state->roident == node) {
            LWLockAcquire(&state->lock, LW_SHARED);

            remote_lsn = state->remote_lsn;
            local_lsn = state->local_lsn;

            LWLockRelease(&state->lock);

            break;
        }
    }

    LWLockRelease(ReplicationOriginLock);

    if (flush && local_lsn != InvalidXLogRecPtr)
        XLogWaitFlush(local_lsn);

    return remote_lsn;
}

/*
 * Tear down a (possibly) configured session replication origin during process
 * exit.
 */
static void ReplicationOriginExitCleanup(int code, Datum arg)
{
    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    if (u_sess->reporigin_cxt.curRepState != NULL &&
        u_sess->reporigin_cxt.curRepState->acquired_by == t_thrd.proc_cxt.MyProcPid) {
        u_sess->reporigin_cxt.curRepState->acquired_by = 0;
        u_sess->reporigin_cxt.curRepState = NULL;
    }

    LWLockRelease(ReplicationOriginLock);
}

/*
 * Setup a replication origin in the shared memory struct if it doesn't
 * already exists and cache access to the specific ReplicationSlot so the
 * array doesn't have to be searched when calling
 * replorigin_session_advance().
 *
 * Obviously only one such cached origin can exist per process and the current
 * cached value can only be set again after the previous value is torn down
 * with replorigin_session_reset().
 */
void replorigin_session_setup(RepOriginId node)
{
    int i;
    int free_slot = -1;

    if (!u_sess->reporigin_cxt.registeredCleanup) {
        on_shmem_exit(ReplicationOriginExitCleanup, 0);
        u_sess->reporigin_cxt.registeredCleanup = true;
    }

    Assert(g_instance.attr.attr_storage.max_replication_slots > 0);

    if (u_sess->reporigin_cxt.curRepState != NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("cannot setup replication origin when one is already setup")));

    /* Lock exclusively, as we may have to create a new table entry. */
    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    /*
     * Search for either an existing slot for the origin, or a free one we can
     * use.
     */
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationState *curstate = &u_sess->reporigin_cxt.repStatesShm->states[i];

        /* remember where to insert if necessary */
        if (curstate->roident == InvalidRepOriginId && free_slot == -1) {
            free_slot = i;
            continue;
        }

        /* not our slot */
        if (curstate->roident != node)
            continue;

        else if (curstate->acquired_by != 0) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg(
                "replication identifier %d is already active for PID %lu", curstate->roident, curstate->acquired_by)));
        }

        /* ok, found slot */
        u_sess->reporigin_cxt.curRepState = curstate;
    }

    if (u_sess->reporigin_cxt.curRepState == NULL && free_slot == -1)
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            errmsg("could not find free replication state slot for replication origin with OID %u", node),
            errhint("Increase max_replication_slots and try again.")));
    else if (u_sess->reporigin_cxt.curRepState == NULL) {
        /* initialize new slot */
        u_sess->reporigin_cxt.curRepState = &u_sess->reporigin_cxt.repStatesShm->states[free_slot];
        Assert(u_sess->reporigin_cxt.curRepState->remote_lsn == InvalidXLogRecPtr);
        Assert(u_sess->reporigin_cxt.curRepState->local_lsn == InvalidXLogRecPtr);
        u_sess->reporigin_cxt.curRepState->roident = node;
    }

    Assert(u_sess->reporigin_cxt.curRepState->roident != InvalidRepOriginId);

    u_sess->reporigin_cxt.curRepState->acquired_by = t_thrd.proc_cxt.MyProcPid;

    LWLockRelease(ReplicationOriginLock);
}

/*
 * Reset replay state previously setup in this session.
 *
 * This function may only be called if an origin was setup with
 * replorigin_session_setup().
 */
static void replorigin_session_reset(void)
{
    Assert(g_instance.attr.attr_storage.max_replication_slots != 0);

    if (u_sess->reporigin_cxt.curRepState == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("no replication origin is configured")));

    LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);

    u_sess->reporigin_cxt.curRepState->acquired_by = 0;
    u_sess->reporigin_cxt.curRepState = NULL;

    LWLockRelease(ReplicationOriginLock);
}

/*
 * Do the same work replorigin_advance() does, just on the session's
 * configured origin.
 *
 * This is noticeably cheaper than using replorigin_advance().
 */
void replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit)
{
    Assert(u_sess->reporigin_cxt.curRepState != NULL);
    Assert(u_sess->reporigin_cxt.curRepState->roident != InvalidRepOriginId);

    LWLockAcquire(&u_sess->reporigin_cxt.curRepState->lock, LW_EXCLUSIVE);
    if (u_sess->reporigin_cxt.curRepState->local_lsn < local_commit)
        u_sess->reporigin_cxt.curRepState->local_lsn = local_commit;
    if (u_sess->reporigin_cxt.curRepState->remote_lsn < remote_commit)
        u_sess->reporigin_cxt.curRepState->remote_lsn = remote_commit;
    LWLockRelease(&u_sess->reporigin_cxt.curRepState->lock);
}

/*
 * Ask the machinery about the point up to which we successfully replayed
 * changes from an already setup replication origin.
 */
XLogRecPtr replorigin_session_get_progress(bool flush)
{
    XLogRecPtr remote_lsn;
    XLogRecPtr local_lsn;

    Assert(u_sess->reporigin_cxt.curRepState != NULL);

    LWLockAcquire(&u_sess->reporigin_cxt.curRepState->lock, LW_SHARED);
    remote_lsn = u_sess->reporigin_cxt.curRepState->remote_lsn;
    local_lsn = u_sess->reporigin_cxt.curRepState->local_lsn;
    LWLockRelease(&u_sess->reporigin_cxt.curRepState->lock);

    if (flush && local_lsn != InvalidXLogRecPtr)
        XLogWaitFlush(local_lsn);

    return remote_lsn;
}


/* ---------------------------------------------------------------------------
 * SQL functions for working with replication origin.
 *
 * These mostly should be fairly short wrappers around more generic functions.
 * ---------------------------------------------------------------------------
 */

/*
 * Create replication origin for the passed in name, and return the assigned
 * oid.
 */
Datum pg_replication_origin_create(PG_FUNCTION_ARGS)
{
    char *name;
    RepOriginId roident;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring((text *)DatumGetPointer(PG_GETARG_DATUM(0)));
    /* Replication origins "pg_xxx" are reserved for internal use */
    if (IsReservedName(name))
        ereport(ERROR, (errcode(ERRCODE_RESERVED_NAME),
                 errmsg("replication origin name \"%s\" is reserved", name),
                 errdetail("Origin names starting with \"pg_\" are reserved.")));
    roident = replorigin_create(name);

    pfree(name);

    PG_RETURN_OID(roident);
}

/*
 * Drop replication origin.
 */
Datum pg_replication_origin_drop(PG_FUNCTION_ARGS)
{
    char *name;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring((text *)DatumGetPointer(PG_GETARG_DATUM(0)));

    replorigin_drop_by_name(name, false);

    pfree(name);

    PG_RETURN_VOID();
}

/*
 * Return oid of a replication origin.
 */
Datum pg_replication_origin_oid(PG_FUNCTION_ARGS)
{
    char *name;
    RepOriginId roident;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring((text *)DatumGetPointer(PG_GETARG_DATUM(0)));
    roident = replorigin_by_name(name, true);

    pfree(name);

    if (OidIsValid(roident))
        PG_RETURN_OID(roident);
    PG_RETURN_NULL();
}

/*
 * Setup a replication origin for this session.
 */
Datum pg_replication_origin_session_setup(PG_FUNCTION_ARGS)
{
    char *name;
    RepOriginId origin;

    replorigin_check_prerequisites(true, false);

    name = text_to_cstring((text *)DatumGetPointer(PG_GETARG_DATUM(0)));
    origin = replorigin_by_name(name, false);
    replorigin_session_setup(origin);

    u_sess->reporigin_cxt.originId = origin;

    pfree(name);

    PG_RETURN_VOID();
}

/*
 * Reset previously setup origin in this session
 */
Datum pg_replication_origin_session_reset(PG_FUNCTION_ARGS)
{
    replorigin_check_prerequisites(true, false);

    replorigin_session_reset();

    u_sess->reporigin_cxt.originId = InvalidRepOriginId;
    u_sess->reporigin_cxt.originLsn = InvalidXLogRecPtr;
    u_sess->reporigin_cxt.originTs = 0;

    PG_RETURN_VOID();
}

/*
 * Has a replication origin been setup for this session.
 */
Datum pg_replication_origin_session_is_setup(PG_FUNCTION_ARGS)
{
    replorigin_check_prerequisites(false, false);

    PG_RETURN_BOOL(u_sess->reporigin_cxt.originId != InvalidRepOriginId);
}


/*
 * Return the replication progress for origin setup in the current session.
 *
 * If 'flush' is set to true it is ensured that the returned value corresponds
 * to a local transaction that has been flushed. this is useful if asychronous
 * commits are used when replaying replicated transactions.
 */
Datum pg_replication_origin_session_progress(PG_FUNCTION_ARGS)
{
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;
    bool flush = PG_GETARG_BOOL(0);

    replorigin_check_prerequisites(true, false);

    if (u_sess->reporigin_cxt.curRepState == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("no replication origin is configured")));

    remote_lsn = replorigin_session_get_progress(flush);
    if (remote_lsn == InvalidXLogRecPtr)
        PG_RETURN_NULL();

    char remote_lsn_s[MAXFNAMELEN];
    int nRet = snprintf_s(remote_lsn_s, sizeof(remote_lsn_s), sizeof(remote_lsn_s) - 1, "%X/%X",
                            (uint32)(remote_lsn >> 32), (uint32)remote_lsn);
    securec_check_ss(nRet, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(remote_lsn_s));
}

Datum pg_replication_origin_xact_setup(PG_FUNCTION_ARGS)
{
    XLogRecPtr location = InvalidXLogRecPtr;
    const char *str_location = TextDatumGetCString(PG_GETARG_DATUM(0));
    ValidateName(str_location);
    if (!AssignLsn(&location, str_location)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg("invalid input syntax for type lsn: \"%s\" of start_lsn", str_location)));
    }

    replorigin_check_prerequisites(true, false);

    if (u_sess->reporigin_cxt.curRepState == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("no replication origin is configured")));

    u_sess->reporigin_cxt.originLsn = location;
    u_sess->reporigin_cxt.originTs = PG_GETARG_TIMESTAMPTZ(1);

    PG_RETURN_VOID();
}

Datum pg_replication_origin_xact_reset(PG_FUNCTION_ARGS)
{
    replorigin_check_prerequisites(true, false);

    u_sess->reporigin_cxt.originLsn = InvalidXLogRecPtr;
    u_sess->reporigin_cxt.originTs = 0;

    PG_RETURN_VOID();
}


Datum pg_replication_origin_advance(PG_FUNCTION_ARGS)
{
    text *name = PG_GETARG_TEXT_P(0);
    XLogRecPtr remote_commit = InvalidXLogRecPtr;
    const char *str_remote_commit = TextDatumGetCString(PG_GETARG_DATUM(1));
    ValidateName(str_remote_commit);
    if (!AssignLsn(&remote_commit, str_remote_commit)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type lsn: \"%s\" "
                        "of start_lsn",
                        str_remote_commit)));
    }

    RepOriginId node;

    replorigin_check_prerequisites(true, false);

    /* lock to prevent the replication origin from vanishing */
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    char *originName = text_to_cstring(name);
    node = replorigin_by_name(originName, false);
    pfree(originName);

    /*
     * Can't sensibly pass a local commit to be flushed at checkpoint - this
     * xact hasn't committed yet. This is why this function should be used to
     * set up the initial replication state, but not for replay.
     */
    replorigin_advance(node, remote_commit, InvalidXLogRecPtr, true /* go backward */, true /* wal log */);

    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    PG_RETURN_VOID();
}


/*
 * Return the replication progress for an individual replication origin.
 *
 * If 'flush' is set to true it is ensured that the returned value corresponds
 * to a local transaction that has been flushed. this is useful if asychronous
 * commits are used when replaying replicated transactions.
 */
Datum pg_replication_origin_progress(PG_FUNCTION_ARGS)
{
    char *name;
    bool flush;
    RepOriginId roident;
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;

    replorigin_check_prerequisites(true, true);

    name = text_to_cstring((text *)DatumGetPointer(PG_GETARG_DATUM(0)));
    flush = PG_GETARG_BOOL(1);

    roident = replorigin_by_name(name, false);
    Assert(OidIsValid(roident));

    pfree(name);
    remote_lsn = replorigin_get_progress(roident, flush);
    if (remote_lsn == InvalidXLogRecPtr)
        PG_RETURN_NULL();

    char remote_lsn_s[MAXFNAMELEN];
    int nRet = snprintf_s(remote_lsn_s, sizeof(remote_lsn_s), sizeof(remote_lsn_s) - 1, "%X/%X",
                            (uint32)(remote_lsn >> 32), (uint32)remote_lsn);
    securec_check_ss(nRet, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(remote_lsn_s));
}


Datum pg_show_replication_origin_status(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int i;
    int rc;

    /* we we want to return 0 rows if slot is set to zero */
    replorigin_check_prerequisites(false, true);

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    if (tupdesc->natts != REPLICATION_ORIGIN_PROGRESS_COLS)
        elog(ERROR, "wrong function definition");

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    /* prevent slots from being concurrently dropped */
    LWLockAcquire(ReplicationOriginLock, LW_SHARED);

    /*
     * Iterate through all possible replication_states, display if they are
     * filled. Note that we do not take any locks, so slightly corrupted/out
     * of date values are a possibility.
     */
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationState *state;
        Datum values[REPLICATION_ORIGIN_PROGRESS_COLS];
        bool nulls[REPLICATION_ORIGIN_PROGRESS_COLS];
        char *roname;
        int idx = 0;

        state = &u_sess->reporigin_cxt.repStatesShm->states[i];

        /* unused slot, nothing to display */
        if (state->roident == InvalidRepOriginId)
            continue;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
        securec_check(rc, "", "");

        values[idx] = ObjectIdGetDatum(state->roident);
        nulls[idx++] = false;

        /*
         * We're not preventing the origin to be dropped concurrently, so
         * silently accept that it might be gone.
         */
        if (replorigin_by_oid(state->roident, true, &roname)) {
            values[idx] = CStringGetTextDatum(roname);
            nulls[idx] = false;
        }
        idx++;

        LWLockAcquire(&state->lock, LW_SHARED);

        char remote_lsn_s[MAXFNAMELEN];
        int nRet = 0;
        nRet = snprintf_s(remote_lsn_s, sizeof(remote_lsn_s), sizeof(remote_lsn_s) - 1, "%X/%X",
                                (uint32)(state->remote_lsn >> BITS_PER_INT), (uint32)state->remote_lsn);
        securec_check_ss(nRet, "\0", "\0");
        values[idx] = CStringGetTextDatum(remote_lsn_s);
        nulls[idx++] = false;

        char local_lsn_s[MAXFNAMELEN];
        nRet = snprintf_s(local_lsn_s, sizeof(local_lsn_s), sizeof(local_lsn_s) - 1, "%X/%X",
                                (uint32)(state->local_lsn >> BITS_PER_INT), (uint32)state->local_lsn);
        securec_check_ss(nRet, "\0", "\0");
        values[idx] = CStringGetTextDatum(local_lsn_s);
        nulls[idx++] = false;

        LWLockRelease(&state->lock);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);

    LWLockRelease(ReplicationOriginLock);

#undef REPLICATION_ORIGIN_PROGRESS_COLS

    return (Datum)0;
}
