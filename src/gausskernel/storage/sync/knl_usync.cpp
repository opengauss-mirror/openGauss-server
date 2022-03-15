/* -------------------------------------------------------------------------
 *
 * knl_usync.cpp
 *        File synchronization management code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/sync/knl_usync.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "miscadmin.h"
#include "pgstat.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "commands/tablespace.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"

/*
 * In some contexts (currently, standalone backends and the checkpointer)
 * we keep track of pending fsync operations: we need to remember all relation
 * segments that have been written since the last checkpoint, so that we can
 * fsync them down to disk before completing the next checkpoint.  This hash
 * table remembers the pending operations.  We use a hash table mostly as
 * a convenient way of merging duplicate requests.
 *
 * We use a similar mechanism to remember no-longer-needed files that can
 * be deleted after the next checkpoint, but we use a linked list instead of
 * a hash table, because we don't expect there to be any duplicate requests.
 *
 * These mechanisms are only used for non-temp relations; we never fsync
 * temp rels, nor do we need to postpone their deletion (see comments in
 * mdunlink).
 *
 * (Regular backends do not track pending operations locally, but forward
 * them to the checkpointer.)
 */
typedef uint16 CycleCtr;        /* can be any convenient integer size */

typedef struct {
    FileTag tag;        /* identifies handler and file */
    CycleCtr cycle_ctr; /* sync_cycle_ctr of oldest request */
    bool canceled;      /* canceled is true if we canceled "recently" */
} PendingFsyncEntry;

typedef struct {
    FileTag tag;        /* identifies handler and file */
    CycleCtr cycle_ctr; /* checkpoint_cycle_ctr when request was made */
} PendingUnlinkEntry;

/* Intervals for calling AbsorbSyncRequests */
const int FSYNCS_PER_ABSORB = 10;
const int UNLINKS_PER_ABSORB = 10;

/* time conversion */
static const int MSEC_PER_MICROSEC = 1000;

/*
 * Function pointers for handling sync and unlink requests.
 */
typedef struct SyncOps {
    int (*syncfiletag)(const FileTag *ftag, char *path);
    int (*unlinkfiletag)(const FileTag *ftag, char *path);
    bool (*matchfiletag)(const FileTag *ftag, const FileTag *candidate);
    void (*sync_forgetdb_fsync)(Oid dbid);
} SyncOps;

static const SyncOps SYNCSW[] = {
    /* magnetic disk */
    {
        .syncfiletag = SyncMdFile,
        .unlinkfiletag = UnlinkMdFile,
        .matchfiletag = MatchMdFileTag,
        .sync_forgetdb_fsync = mdForgetDatabaseFsyncRequests,
    },
    /* undo log segment files */
    {
        .syncfiletag = SyncUndoFile,
        .unlinkfiletag = SyncUnlinkUndoFile,
        .matchfiletag = NULL,
        .sync_forgetdb_fsync = NULL,
    },
    /* segment store */
    {
        .syncfiletag = seg_sync_filetag,
        .unlinkfiletag = seg_unlink_filetag,
        .matchfiletag = seg_filetag_matches,
        .sync_forgetdb_fsync = segForgetDatabaseFsyncRequests,
    },
};

static const int NSync = lengthof(SYNCSW);

void InitPendingOps(void)
{
    if (!IsUnderPostmaster || AmStartupProcess() || AmCheckpointerProcess() || AmPageWriterMainProcess()) {
        HASHCTL     hashCtl;
        errno_t rc;

        /*
         * XXX: The checkpointer needs to add entries to the pending ops table
         * when absorbing fsync requests.  That is done within a critical
         * section, which isn't usually allowed, but we make an exception. It
         * means that there's a theoretical possibility that you run out of
         * memory while absorbing fsync requests, which leads to a PANIC.
         * Fortunately the hash table is small so that's unlikely to happen in
         * practice.
         */
        u_sess->storage_cxt.pendingOpsCxt = AllocSetContextCreate(u_sess->top_mem_cxt,
            "Pending ops context", ALLOCSET_DEFAULT_SIZES);
        MemoryContextAllowInCriticalSection(u_sess->storage_cxt.pendingOpsCxt, true);
        rc = memset_s(&hashCtl, sizeof(hashCtl), 0, sizeof(hashCtl));
        securec_check(rc, "", "");
        hashCtl.keysize = sizeof(FileTag);
        hashCtl.entrysize = sizeof(PendingFsyncEntry);
        hashCtl.hcxt = u_sess->storage_cxt.pendingOpsCxt;
        hashCtl.hash = tag_hash;
        u_sess->storage_cxt.pendingOps = hash_create("Pending Ops Table",
            100L, &hashCtl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }
}

/*
 * Initialize data structures for the file sync tracking.
 */
void InitSync(void)
{
    /*
     * Create pending-operations hashtable if we need it.  Currently, we need
     * it if we are standalone (not under a postmaster) or if we are a startup
     * or checkpointer auxiliary process.
     */
    if (!IsUnderPostmaster || AmStartupProcess() || AmCheckpointerProcess() || AmPageWriterMainProcess()) {
        InitPendingOps();
        u_sess->storage_cxt.pendingUnlinks = NIL;
    }
}

/*
 * SyncPreCheckpoint() -- Do pre-checkpoint work
 *
 * To distinguish unlink requests that arrived before this checkpoint
 * started from those that arrived during the checkpoint, we use a cycle
 * counter similar to the one we use for fsync requests. That cycle
 * counter is incremented here.
 *
 * This must be called *before* the checkpoint REDO point is determined.
 * That ensures that we won't delete files too soon.
 *
 * Note that we can't do anything here that depends on the assumption
 * that the checkpoint will be completed.
 */
void SyncPreCheckpoint(void)
{
    /*
     * Any unlink requests arriving after this point will be assigned the next
     * cycle counter, and won't be unlinked until next checkpoint.
     */
    u_sess->storage_cxt.checkpoint_cycle_ctr++;
}

/*
 * SyncPostCheckpoint() -- Do post-checkpoint work
 *
 * Remove any lingering files that can now be safely removed.
 */
void SyncPostCheckpoint(void)
{
    int absorbCounter;

    absorbCounter = UNLINKS_PER_ABSORB;
    while (u_sess->storage_cxt.pendingUnlinks != NIL) {
        PendingUnlinkEntry *entry = (PendingUnlinkEntry *) linitial(u_sess->storage_cxt.pendingUnlinks);
        char path[MAXPGPATH];

        /*
         * New entries are appended to the end, so if the entry is new we've
         * reached the end of old entries.
         *
         * Note: if just the right number of consecutive checkpoints fail, we
         * could be fooled here by cycle_ctr wraparound.  However, the only
         * consequence is that we'd delay unlinking for one more checkpoint,
         * which is perfectly tolerable.
         */
        if (entry->cycle_ctr == u_sess->storage_cxt.checkpoint_cycle_ctr) {
            break;
        }

        /* Unlink the file */
        if (SYNCSW[entry->tag.handler].unlinkfiletag(&entry->tag, path) < 0) {
            /*
             * There's a race condition, when the database is dropped at the
             * same time that we process the pending unlink requests. If the
             * DROP DATABASE deletes the file before we do, we will get ENOENT
             * here. rmtree() also has to ignore ENOENT errors, to deal with
             * the possibility that we delete the file first.
             */
            if (errno != ENOENT) {
                ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not remove file \"%s\": %m", path)));
            }
        }

        /* And remove the list entry */
        u_sess->storage_cxt.pendingUnlinks = list_delete_first(u_sess->storage_cxt.pendingUnlinks);
        pfree(entry);

        /*
         * As in ProcessSyncRequests, we don't want to stop absorbing fsync
         * requests for along time when there are many deletions to be done.
         * We can safely call AbsorbSyncRequests() at this point in the loop
         * (note it might try to delete list entries).
         */
        if (--absorbCounter <= 0) {
            CkptAbsorbFsyncRequests();
            absorbCounter = UNLINKS_PER_ABSORB;
        }
    }

    /*
     * 1. incremental checkpoint mode, checkpoint thread need reset the pendingOps hashtable;
     * 2. full checkpoint mode, call ProcessSyncRequests, can remove the entry from pendingOps.
     */
    if (ENABLE_INCRE_CKPT) {
        hash_destroy(u_sess->storage_cxt.pendingOps);
        InitPendingOps();
    }
}

static void AbsorbFsyncRequests(void)
{
    if (AmPageWriterMainProcess()) {
        PgwrAbsorbFsyncRequests();
    } else {
        CkptAbsorbFsyncRequests();
    }
}

static void HandleAbnormalSyncExit(bool syncInProgress)
{
    HASH_SEQ_STATUS hstat;
    PendingFsyncEntry *entry;

    /*
     * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
     * checkpoint), we want to ignore fsync requests that are entered into the
     * hashtable after this point --- they should be processed next time,
     * instead.  We use sync_cycle_ctr to tell old entries apart from new
     * ones: new ones will have cycle_ctr equal to the incremented value of
     * sync_cycle_ctr.
     *
     * In normal circumstances, all entries present in the table at this point
     * will have cycle_ctr exactly equal to the current (about to be old)
     * value of sync_cycle_ctr.  However, if we fail partway through the
     * fsync'ing loop, then older values of cycle_ctr might remain when we
     * come back here to try again.  Repeated checkpoint failures would
     * eventually wrap the counter around to the point where an old entry
     * might appear new, causing us to skip it, possibly allowing a checkpoint
     * to succeed that should not have.  To forestall wraparound, any time the
     * previous ProcessSyncRequests() failed to complete, run through the
     * table and forcibly set cycle_ctr = sync_cycle_ctr.
     *
     * Think not to merge this loop with the main loop, as the problem is
     * exactly that that loop may fail before having visited all the entries.
     * From a performance point of view it doesn't matter anyway, as this path
     * will never be taken in a system that's functioning normally.
     */
    if (syncInProgress) {
        /* prior try failed, so update any stale cycle_ctr values */
        hash_seq_init(&hstat, u_sess->storage_cxt.pendingOps);
        while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL) {
            entry->cycle_ctr = u_sess->storage_cxt.sync_cycle_ctr;
        }
    }
    return;
}

/*
 *  ProcessSyncRequests() -- Process queued fsync requests.
 */
void ProcessSyncRequests(void)
{
    static bool syncInProgress = false;
    HASH_SEQ_STATUS hstat;
    PendingFsyncEntry *entry;
    int absorbCounter;

    /* Statistics on sync times */
    int         processed = 0;
    instr_time  syncStart;
    instr_time  syncEnd;
    instr_time  syncDiff;
    uint64      elapsed;
    uint64      longest = 0;
    uint64      totalElapsed = 0;

    /*
     * This is only called during checkpoints, and checkpoints should only
     * occur in processes that have created a pendingOps.
     */
    if (!u_sess->storage_cxt.pendingOps) {
        ereport(ERROR, (errmsg("cannot sync without a pendingOps table")));
    }

    /*
     * If we are in the checkpointer, the sync had better include all fsync
     * requests that were queued by backends up to this point.  The tightest
     * race condition that could occur is that a buffer that must be written
     * and fsync'd for the checkpoint could have been dumped by a backend just
     * before it was visited by BufferSync().  We know the backend will have
     * queued an fsync request before clearing the buffer's dirtybit, so we
     * are safe as long as we do an Absorb after completing BufferSync().
     */
    AbsorbFsyncRequests();

    HandleAbnormalSyncExit(syncInProgress);
    /* Advance counter so that new hashtable entries are distinguishable */
    u_sess->storage_cxt.sync_cycle_ctr++;

    /* Set flag to detect failure if we don't reach the end of the loop */
    syncInProgress = true;

    /* Now scan the hashtable for fsync requests to process */
    absorbCounter = FSYNCS_PER_ABSORB;
    hash_seq_init(&hstat, u_sess->storage_cxt.pendingOps);
    while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL) {
        int failures;

        /*
         * If fsync is off then we don't have to bother opening the file at
         * all.  (We delay checking until this point so that changing fsync on
         * the fly behaves sensibly.)
         */
        if (!u_sess->attr.attr_storage.enableFsync) {
            continue;
        }

        /*
         * If the entry is new then don't process it this time; it is new.
         * Note "continue" bypasses the hash-remove call at the bottom of the
         * loop.
         */
        if (entry->cycle_ctr == u_sess->storage_cxt.sync_cycle_ctr) {
            continue;
        }

        /* Else assert we haven't missed it */
        Assert((CycleCtr) (entry->cycle_ctr + 1) == u_sess->storage_cxt.sync_cycle_ctr);

        /*
         * If in checkpointer, we want to absorb pending requests every so
         * often to prevent overflow of the fsync request queue.  It is
         * unspecified whether newly-added entries will be visited by
         * hash_seq_search, but we don't care since we don't need to process
         * them anyway.
         */
        if (--absorbCounter <= 0) {
            AbsorbFsyncRequests();
            absorbCounter = FSYNCS_PER_ABSORB;
        }

        /*
         * The fsync table could contain requests to fsync segments that have
         * been deleted (unlinked) by the time we get to them. Rather than
         * just hoping an ENOENT (or EACCES on Windows) error can be ignored,
         * what we do on error is absorb pending requests and then retry.
         * Since mdunlink() queues a "cancel" message before actually
         * unlinking, the fsync request is guaranteed to be marked canceled
         * after the absorb if it really was this case. DROP DATABASE likewise
         * has to tell us to forget fsync requests before it starts deletions.
         */
        for (failures = 0; !entry->canceled; failures++) {
            char path[MAXPGPATH];

            INSTR_TIME_SET_CURRENT(syncStart);
            if (SYNCSW[entry->tag.handler].syncfiletag(&entry->tag, path) == 0) {
                /* Success; update statistics about sync timing */
                INSTR_TIME_SET_CURRENT(syncEnd);
                syncDiff = syncEnd;
                INSTR_TIME_SUBTRACT(syncDiff, syncStart);
                elapsed = INSTR_TIME_GET_MICROSEC(syncDiff);
                if (elapsed > longest) {
                    longest = elapsed;
                }
                totalElapsed += elapsed;
                processed++;

                if (u_sess->attr.attr_common.log_checkpoints) {
                    ereport(DEBUG1, (errmsg("checkpoint sync: number=%d file=%s time=%.3f msec",
                        processed, path, (double) elapsed / MSEC_PER_MICROSEC)));
                }
                break;          /* out of retry loop */
            }

            /*
             * It is possible that the relation has been dropped or truncated
             * since the fsync request was entered. Therefore, allow ENOENT,
             * but only if we didn't fail already on this file.
             */
            if (!FILE_POSSIBLY_DELETED(errno) || failures > 0) {
                if (check_unlink_rel_hashtbl(entry->tag.rnode, entry->tag.forknum)) {
                    ereport(DEBUG1,
                        (errmsg("could not fsync file \"%s\": %m, this relation has been remove", path)));
                    break;
                }

                /*
                 * Absorb incoming requests and check to see if a cancel arrived
                 * for this relation fork.
                 */
                AbsorbFsyncRequests();
                if (entry->canceled) {
                    break;
                }
                /* treat it as truncate */
                ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
                    errmsg("could not fsync file \"%s\": %m", path)));
                break;
            } else {
                ereport(DEBUG1,
                        (errcode_for_file_access(),
                         errmsg("could not fsync file \"%s\" but retrying: %m", path)));
            }

            /*
             * Absorb incoming requests and check to see if a cancel arrived
             * for this relation fork.
             */
            AbsorbFsyncRequests();
            absorbCounter = FSYNCS_PER_ABSORB; /* might as well... */
        }

        /* We are done with this entry, remove it */
        if (hash_search(u_sess->storage_cxt.pendingOps, &entry->tag, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errmsg("pendingOps corrupted")));
        }
    }

    /* Return sync performance metrics for report at checkpoint end */
    if (!AmPageWriterMainProcess()) {
        t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels = processed;
        t_thrd.xlog_cxt.CheckpointStats->ckpt_longest_sync = longest;
        t_thrd.xlog_cxt.CheckpointStats->ckpt_agg_sync_time = totalElapsed;
    }

    /* Flag successful completion of ProcessSyncRequests */
    syncInProgress = false;
}

void ProcessUnlinkList(const FileTag *ftag)
{
    ListCell *cell;
    ListCell *prev;
    ListCell *next;

    if (!AmPageWriterMainProcess()) {
        prev = NULL;
        for (cell = list_head(u_sess->storage_cxt.pendingUnlinks); cell; cell = next) {
            PendingUnlinkEntry *entry = (PendingUnlinkEntry *) lfirst(cell);
            next = lnext(cell);
            if (entry->tag.handler == ftag->handler && SYNCSW[ftag->handler].matchfiletag(ftag, &entry->tag)) {
                u_sess->storage_cxt.pendingUnlinks =
                    list_delete_cell(u_sess->storage_cxt.pendingUnlinks, cell, prev);
                pfree(entry);
            } else {
                prev = cell;
            }
        }
    }
}

/*
 * RememberSyncRequest() -- callback from checkpointer side of sync request
 *
 * We stuff fsync requests into the local hash table for execution
 * during the checkpointer's next checkpoint.  UNLINK requests go into a
 * separate linked list, however, because they get processed separately.
 *
 * See knl_usync.h for more information on the types of sync requests supported.
 */
void RememberSyncRequest(const FileTag *ftag, SyncRequestType type)
{
    Assert(u_sess->storage_cxt.pendingOps);

    if (type == SYNC_FORGET_REQUEST) {
        PendingFsyncEntry *entry;

        /* Cancel previously entered request */
        entry = (PendingFsyncEntry *) hash_search(u_sess->storage_cxt.pendingOps, (void *) ftag, HASH_FIND, NULL);
        if (entry != NULL) {
            entry->canceled = true;
        }
    } else if (type == SYNC_FILTER_REQUEST) {
        HASH_SEQ_STATUS hstat;
        PendingFsyncEntry *entry;

        /* Cancel matching fsync requests */
        hash_seq_init(&hstat, u_sess->storage_cxt.pendingOps);
        while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL) {
            if (entry->tag.handler == ftag->handler &&
                SYNCSW[ftag->handler].matchfiletag(ftag, &entry->tag)) {
                entry->canceled = true;
            }
        }

        /* Remove matching unlink requests, only the checkpoint thread handle the pendingUnlinks */
        ProcessUnlinkList(ftag);
    } else if (type == SYNC_UNLINK_REQUEST) {
        Assert(!AmPageWriterMainProcess());
        /* Unlink request: put it in the linked list */
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->storage_cxt.pendingOpsCxt);
        PendingUnlinkEntry *entry = (PendingUnlinkEntry*)palloc(sizeof(PendingUnlinkEntry));
        entry->tag = *ftag;
        entry->cycle_ctr = u_sess->storage_cxt.checkpoint_cycle_ctr;
        u_sess->storage_cxt.pendingUnlinks = lappend(u_sess->storage_cxt.pendingUnlinks, entry);
        MemoryContextSwitchTo(oldcxt);
    } else {
        /* Normal case: enter a request to fsync this segment */
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->storage_cxt.pendingOpsCxt);
        PendingFsyncEntry *entry;
        bool found = false;

        Assert(type == SYNC_REQUEST);

        entry = (PendingFsyncEntry *) hash_search(u_sess->storage_cxt.pendingOps, (void *) ftag, HASH_ENTER, &found);
        /* if new entry, initialize it */
        if (!found) {
            entry->cycle_ctr = u_sess->storage_cxt.sync_cycle_ctr;
            entry->canceled = false;
            entry->tag = *ftag; // ZheapTodo initialize tag first
        }

        /*
         * NB: it's intentional that we don't change cycle_ctr if the entry
         * already exists.  The cycle_ctr must represent the oldest fsync
         * request that could be in the entry.
         */
        MemoryContextSwitchTo(oldcxt);
    }
}

static bool ForwardSyncRequest(const FileTag *ftag, SyncRequestType type)
{
    bool ret = false;

    /*
     * Notify the checkpointer about it.  If we fail to queue a message in
     * retryOnError mode, we have to sleep and try again ... ugly, but
     * hopefully won't happen often.
     *
     * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with an
     * error in the case of SYNC_UNLINK_REQUEST would leave the
     * no-longer-used file still present on disk, which would be bad, so
     * I'm inclined to assume that the checkpointer will always empty the
     * queue soon.
     */
    if (USE_CKPT_THREAD_SYNC) {
        ret = CkptForwardSyncRequest(ftag, type);
    } else {
        switch (type) {
            case SYNC_REQUEST:
                ret = PgwrForwardSyncRequest(ftag, type);
                break;
            case SYNC_UNLINK_REQUEST:
                ret = CkptForwardSyncRequest(ftag, type);
                break;
            case SYNC_FORGET_REQUEST:
                ret = PgwrForwardSyncRequest(ftag, type);
                break;
            case SYNC_FILTER_REQUEST:
                ret = PgwrForwardSyncRequest(ftag, type);
                if (!ret) {
                    break;
                }
                ret = CkptForwardSyncRequest(ftag, type);
                break;
            default:
                ereport(ERROR,
                    (errmsg("Incremental ckpt, Error SyncRequestType, the type is %d", type)));
                break;
        }
    }
    return ret;
}

/*
 * Register the sync request locally, or forward it to the checkpointer.
 *
 * If retryOnError is true, we'll keep trying if there is no space in the
 * queue.  Return true if we succeeded, or false if there wasn't space.
 */
bool RegisterSyncRequest(const FileTag *ftag, SyncRequestType type, bool retryOnError)
{
    bool ret = false;

    if (u_sess->storage_cxt.pendingOps != NULL) {
        /* standalone backend or startup process: fsync state is local */
        RememberSyncRequest(ftag, type);
        return true;
    }

    for (;;) {
        /*
         * Incremental checkpoint mode, notify the pagewriter about it,
         * full checkpoint mode, notify checkpointer about it.  If we fail
         * to queue a message in retryOnError mode, we have to sleep
         * and try again ... ugly, but hopefully won't happen often.
         *
         * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with an
         * error in the case of SYNC_UNLINK_REQUEST would leave the
         * no-longer-used file still present on disk, which would be bad, so
         * I'm inclined to assume that the checkpointer will always empty the
         * queue soon.
         */
        ret = ForwardSyncRequest(ftag, type);
        /*
         * If we are successful in queueing the request, or we failed and were
         * instructed not to retry on error, break.
         */
        if (ret || !retryOnError) {
            break;
        }
        pg_usleep(10000L);
    }

    return ret;
}

/*
 * In archive recovery, we rely on checkpointer to do fsyncs, but we will have
 * already created the pendingOps during initialization of the startup
 * process.  Calling this function drops the local pendingOps so that
 * subsequent requests will be forwarded to checkpointer.
 */
void EnableSyncRequestForwarding(void)
{
    /* Perform any pending fsyncs we may have queued up, then drop table */
    if (u_sess->storage_cxt.pendingOps) {
        ProcessSyncRequests();
        hash_destroy(u_sess->storage_cxt.pendingOps);
    }
    u_sess->storage_cxt.pendingOps = NULL;

    /*
     * We should not have any pending unlink requests, since mdunlink doesn't
     * queue unlink requests when isRedo.
     */
    Assert(u_sess->storage_cxt.pendingUnlinks == NIL);
}

void ForgetDatabaseSyncRequests(Oid dbid)
{
    /*
     * We need two tags to forget two kinds of fsync requests generated by segment store and heap store respectively
     */
    for (int i=0; i<NSync; i++) {
        if (SYNCSW[i].sync_forgetdb_fsync != NULL) {
            (*(SYNCSW[i].sync_forgetdb_fsync))(dbid);
        }
    }
}

/*
 * Because pagewriter.cpp exceends the maximum file line limit, some functions about checkpoint are moved here
 */

const float HALF = 0.5;
static bool CompactPageWriterRequestQueue(void)
{
    bool* skip_slot = NULL;
    int preserve_count = 0;
    int num_skipped = 0;
    IncreCkptSyncShmemStruct *incre_ckpt_sync_shmem = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;

    /* Initialize skip_slot array */
    skip_slot = (bool*)palloc0(sizeof(bool) * incre_ckpt_sync_shmem->num_requests);

    /* must hold the request queue in exclusive mode */
    Assert(LWLockHeldByMe(incre_ckpt_sync_shmem->sync_queue_lwlock));

    num_skipped = getDuplicateRequest(incre_ckpt_sync_shmem->requests, incre_ckpt_sync_shmem->num_requests, skip_slot);
    if (num_skipped == 0) {
        pfree(skip_slot);
        return false;
    }

    /* We found some duplicates; remove them. */
    for (int n = 0; n < incre_ckpt_sync_shmem->num_requests; n++) {
        if (skip_slot[n])
            continue;
        incre_ckpt_sync_shmem->requests[preserve_count++] = incre_ckpt_sync_shmem->requests[n];
    }

    ereport(DEBUG1,
        (errmsg("pagewriter compacted fsync request queue from %d entries to %d entries",
            incre_ckpt_sync_shmem->num_requests, preserve_count)));

    incre_ckpt_sync_shmem->num_requests = preserve_count;
    pfree(skip_slot);
    return true;
}

 /* PgwrForwardSyncRequest
 *          Forward a file-fsync request from a backend to the pagewriter.
 */
bool PgwrForwardSyncRequest(const FileTag *ftag, SyncRequestType type)
{
    CheckpointerRequest* request = NULL;
    bool too_full = false;
    IncreCkptSyncShmemStruct *incre_ckpt_sync_shmem = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;
    LWLock *sync_queue_lwlock = incre_ckpt_sync_shmem->sync_queue_lwlock;

    if (AmPageWriterMainProcess()) {
        ereport(ERROR,
            (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
                errmsg("PgwrForwardSyncRequest must not be called in pagewriter main thread")));
    }

    LWLockAcquire(sync_queue_lwlock, LW_EXCLUSIVE);

    /*
     * If the pagewriter main thread isn't running or the request queue is full, the
     * backend will have to perform its own fsync request. But before forcing
     * that to happen, we can try to compact the request queue.
     */
    if (incre_ckpt_sync_shmem->pagewritermain_pid == 0 || (incre_ckpt_sync_shmem->num_requests >=
        incre_ckpt_sync_shmem->max_requests && !CompactPageWriterRequestQueue())) {
        LWLockRelease(sync_queue_lwlock);
        return false;
    }

    /* OK, insert request */
    request = &incre_ckpt_sync_shmem->requests[incre_ckpt_sync_shmem->num_requests++];
    request->ftag = *ftag;
    request->type = type;

    /* If queue is more than half full, nudge the pagewriter to empty it */
    too_full = (incre_ckpt_sync_shmem->num_requests >= incre_ckpt_sync_shmem->max_requests * HALF);

    LWLockRelease(sync_queue_lwlock);

    /* wakeup the pagwriter main thread after release lock */
    if (too_full && g_instance.proc_base->pgwrMainThreadLatch) {
        SetLatch(g_instance.proc_base->pgwrMainThreadLatch);
    }

    return true;
}

/* PgwrAbsorbFsyncRequests
 *           pagewriter main thread Absorb the backend or pagewriter sub thread fsync request.
 */
void PgwrAbsorbFsyncRequests(void)
{
    CheckpointerRequest* requests = NULL;
    CheckpointerRequest* request = NULL;
    IncreCkptSyncShmemStruct *incre_ckpt_sync_shmem = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;
    LWLock *sync_queue_lwlock = incre_ckpt_sync_shmem->sync_queue_lwlock;
    int n;

    Assert(AmPageWriterMainProcess());

    /*
     * We have to PANIC if we fail to absorb all the pending requests (eg,
     * because our hashtable runs out of memory).  This is because the system
     * cannot run safely if we are unable to fsync what we have been told to
     * fsync. Fortunately, the hashtable is so small that the problem is
     * quite unlikely to arise in practice.
     */
    START_CRIT_SECTION();

    /* We try to avoid holding the lock for a long time by copying the request array. */
    LWLockAcquire(sync_queue_lwlock, LW_EXCLUSIVE);

    n = incre_ckpt_sync_shmem->num_requests;

    if (n > 0) {
        errno_t rc;
        requests = (CheckpointerRequest*)palloc(n * sizeof(CheckpointerRequest));
        rc = memcpy_s(requests, n * sizeof(CheckpointerRequest), incre_ckpt_sync_shmem->requests,
            n * sizeof(CheckpointerRequest));
        securec_check(rc, "\0", "\0");
    }

    incre_ckpt_sync_shmem->num_requests = 0;

    LWLockRelease(sync_queue_lwlock);

    for (request = requests; n > 0; request++, n--) {
        RememberSyncRequest(&request->ftag, request->type);
    }
    if (requests != NULL) {
        pfree(requests);
    }
    END_CRIT_SECTION();
}

/*
 *  PageWriterSyncWithAbsorption() -- Sync files to disk and reset fsync flags.
 *  incremental checkpoint, pagewriter main thread handle the file sync.
 */
void PageWriterSyncWithAbsorption(void)
{
    volatile IncreCkptSyncShmemStruct* cps = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;

    SpinLockAcquire(&cps->sync_lock);
    cps->fsync_start++;
    SpinLockRelease(&cps->sync_lock);

    ProcessSyncRequests();

    SpinLockAcquire(&cps->sync_lock);
    cps->fsync_done = cps->fsync_start;
    SpinLockRelease(&cps->sync_lock);
}

const int WAIT_THREAD_START = 600;  /* every time sleep 0.1 sec, the 1min = 600 * 0.1 sec */
void RequestPgwrSync(void)
{
    volatile IncreCkptSyncShmemStruct* cps = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;

    /*
     * Send signal to request sync.  It's possible that the pagewriter main thread
     * hasn't started yet, or is in process of restarting, so we will retry a
     * few times if needed.  Also, if not told to wait for the pagewriter main thread to do sync,
     * we consider failure to send the signal to be nonfatal and merely
     * LOG it.
     */
    for (int ntries = 0;; ntries++) {
        if (cps->pagewritermain_pid == 0) {
            /* max wait 1min */
            if (ntries >= WAIT_THREAD_START || pmState == PM_SHUTDOWN) {
                ereport(LOG, (errmsg("could not request pagewriter handle sync because pagewriter not running")));
                break;
            }
        } else if (gs_signal_send(cps->pagewritermain_pid, SIGINT) != 0) {
            /* max wait 1min */
            if (ntries >= WAIT_THREAD_START) {
                ereport(LOG,
                    (errmsg("could not signal for pagewriter main thread: %m, thread pid is %lu",
                        cps->pagewritermain_pid)));
                break;
            }
        } else {
            break; /* signal sent successfully */
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000L); /* wait 0.1 sec, then retry */
    }
    return;
}

/*
 * PageWriterSyncForDw() -- File sync before dw file can be truncated or recycled.
 * Normally, file sync operation is solely handled by pagewirter main process.
 * For standalone backends, as well as for startup process performing dw init,
 * they can handle fsync request.
 */
void PageWriterSync(void)
{
    Assert(ENABLE_INCRE_CKPT);
    /* Incremental checkpoint mode, the checkpoint thread only handle the unlink list */
    if (u_sess->storage_cxt.pendingOps && !AmCheckpointerProcess()) {
        Assert(!IsUnderPostmaster || AmStartupProcess() || AmPageWriterMainProcess());
        ProcessSyncRequests();
    } else {
        int64 old_fsync_start = 0;
        int64 new_fsync_start = 0;
        int64 new_fsync_done = 0;
        volatile IncreCkptSyncShmemStruct* cps = g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem;
        SpinLockAcquire(&cps->sync_lock);
        old_fsync_start = cps->fsync_start;
        SpinLockRelease(&cps->sync_lock);

        RequestPgwrSync();

        /* Wait for a new sync to start. */
        for (;;) {
            SpinLockAcquire(&cps->sync_lock);
            new_fsync_start = cps->fsync_start;
            SpinLockRelease(&cps->sync_lock);

            if (new_fsync_start != old_fsync_start) {
                break;
            }

            CHECK_FOR_INTERRUPTS();
            pg_usleep(100000L);
        }

        /*
         * We are waiting for fsync_done >= new_fsync_start, in a modulo sense.
         */
        for (;;) {
            SpinLockAcquire(&cps->sync_lock);
            new_fsync_done = cps->fsync_done;
            SpinLockRelease(&cps->sync_lock);

            if (new_fsync_done - new_fsync_start >= 0) {
                break;
            }

            CHECK_FOR_INTERRUPTS();
            pg_usleep(100000L);
        }
    }

    return;
}