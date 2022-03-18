/* -------------------------------------------------------------------------
 *
 * bufmgr.cpp
 *	  buffer manager interface routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/buffer/bufmgr.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 * Principal entry points:
 *
 * ReadBuffer() -- find or create a buffer holding the requested page,
 *		and pin it so that no one can destroy it while this process
 *		is using it.
 *
 * ReleaseBuffer() -- unpin a buffer
 *
 * MarkBufferDirty() -- mark a pinned buffer's contents as "dirty".
 *		The disk write is delayed until buffer replacement or checkpoint.
 *
 * See also these files:
 *		freelist.c -- chooses victim for buffer replacement
 *		buf_table.c -- manages the buffer lookup table
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/file.h>

#include "access/xlog.h"
#include "access/cstore_am.h"
#include "access/double_write.h"
#include "access/multi_redo_api.h"
#include "access/transam.h"
#include "access/xlogproc.h"
#include "access/double_write.h"
#include "access/parallel_recovery/dispatcher.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/pg_hashbucket_fn.h"
#include "catalog/storage_gtt.h"
#include "commands/tablespace.h"
#include "commands/verify.h"
#include "executor/instrument.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/aiocompleter.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "service/remote_read_client.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/smgr/segment.h"
#include "storage/standby.h"
#include "utils/aiomem.h"
#include "utils/guc.h"
#include "utils/plog.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"
#include "utils/evp_cipher.h"
#include "replication/walsender_private.h"
#include "replication/walsender.h"
#include "workload/workload.h"
#include "utils/builtins.h"
#include "catalog/pg_namespace.h"
#include "postmaster/pagewriter.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"
#include "tsan_annotation.h"
#include "tde_key_management/tde_key_storage.h"

const int ONE_MILLISECOND = 1;
const int TEN_MICROSECOND = 10;
const int MILLISECOND_TO_MICROSECOND = 1000;
const float PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE = 0.8;
const long CHKPT_LOG_TIME_INTERVAL = 1000000 * 60; /* 60000000us -> 1min */
const double CHKPT_LOG_PERCENT_INTERVAL = 0.1;
const uint32 ESTIMATED_MIN_BLOCKS = 10000;

/*
 * Status of buffers to checkpoint for a particular tablespace, used
 * internally in BufferSync.
 */
typedef struct CkptTsStatus {
    /* oid of the tablespace */
    Oid tsId;

    /*
     * Checkpoint progress for this tablespace. To make progress comparable
     * between tablespaces the progress is, for each tablespace, measured as a
     * number between 0 and the total number of to-be-checkpointed pages. Each
     * page checkpointed in this tablespace increments this space's progress
     * by progress_slice.
     */
    float8 progress;
    float8 progress_slice;

    /* number of to-be checkpointed pages in this tablespace */
    int num_to_scan;
    /* already processed pages in this tablespace */
    int num_scanned;

    /* current offset in CkptBufferIds for this tablespace */
    int index;
} CkptTsStatus;

static inline int32 GetPrivateRefCount(Buffer buffer);
void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);
static void CheckForBufferLeaks(void);
static int ts_ckpt_progress_comparator(Datum a, Datum b, void *arg);
static bool ReadBuffer_common_ReadBlock(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
    BlockNumber blockNum, ReadBufferMode mode, bool isExtend, Block bufBlock, const XLogPhyBlock *pblk,
    bool *need_repair);
static Buffer ReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum, BlockNumber blockNum,
    ReadBufferMode mode, BufferAccessStrategy strategy, bool *hit, const XLogPhyBlock *pblk);


/*
 * Return the PrivateRefCount entry for the passed buffer. It is searched
 * only in PrivateRefCountArray which makes this function very short and
 * suitable to be inline.
 * For complete search, GetPrivateRefCountEntrySlow should be invoked after.
 *
 * Only works for shared buffers.
 */
static PrivateRefCountEntry* GetPrivateRefCountEntryFast(Buffer buffer, PrivateRefCountEntry* &free_entry)
{
    PrivateRefCountEntry* res = NULL;
    int i;

    Assert(BufferIsValid(buffer));
    Assert(!BufferIsLocal(buffer));

    /*
     * First search for references in the array, that'll be sufficient in the
     * majority of cases.
     */
    for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++) {
        res = &t_thrd.storage_cxt.PrivateRefCountArray[i];

        if (res->buffer == buffer) {
            return res;
        }

        /* Remember where to put a new refcount, should it become necessary. */
        if (free_entry == NULL && res->buffer == InvalidBuffer) {
            free_entry = res;
        }
    }
    return NULL;
}

/*
 * Return the PrivateRefCount entry for the passed buffer.
 *
 * This function will be based on the result of GetPrivateRefCountEntryFast
 * to provide complete search, which would be slow.
 *
 * Returns NULL if create = false is passed and the buffer doesn't have a
 * PrivateRefCount entry; allocates a new PrivateRefCountEntry if currently
 * none exists and create = true is passed.
 *
 * If do_move is true - only allowed for create = false - the entry is
 * optimized for frequent access.
 *
 * When a returned refcount entry isn't used anymore it has to be forgotten,
 * using ForgetPrivateRefCountEntry().
 *
 * Only works for shared buffers.
 */
static PrivateRefCountEntry* GetPrivateRefCountEntrySlow(Buffer buffer,
    bool create, bool do_move, PrivateRefCountEntry* free_entry)
{
    Assert(!create || do_move);
    Assert(BufferIsValid(buffer));
    Assert(!BufferIsLocal(buffer));

    /*
     * By here we know that the buffer, if already pinned, isn't residing in
     * the array.
     */
    PrivateRefCountEntry* res = NULL;
    bool found = false;

    /*
     * Look up the buffer in the hashtable if we've previously overflowed into
     * it.
     */
    if (t_thrd.storage_cxt.PrivateRefCountOverflowed > 0) {
        res = (PrivateRefCountEntry *)hash_search(t_thrd.storage_cxt.PrivateRefCountHash, (void *)&buffer, HASH_FIND,
                                                  &found);
    }

    if (!found) {
        if (!create) {
            /* Neither array nor hash have an entry and no new entry is needed */
            return NULL;
        } else if (free_entry != NULL) {
            /* add entry into the free array slot */
            free_entry->buffer = buffer;
            free_entry->refcount = 0;

            return free_entry;
        } else {
            /*
             * Move entry from the current clock position in the array into the
             * hashtable. Use that slot.
             */
            PrivateRefCountEntry *array_ent = NULL;
            PrivateRefCountEntry *hash_ent = NULL;

            /* select victim slot */
            array_ent = &t_thrd.storage_cxt
                             .PrivateRefCountArray[t_thrd.storage_cxt.PrivateRefCountClock++ % REFCOUNT_ARRAY_ENTRIES];
            Assert(array_ent->buffer != InvalidBuffer);

            /* enter victim array entry into hashtable */
            hash_ent = (PrivateRefCountEntry *)hash_search(t_thrd.storage_cxt.PrivateRefCountHash,
                                                           (void *)&array_ent->buffer, HASH_ENTER, &found);
            Assert(!found);
            hash_ent->refcount = array_ent->refcount;

            /* fill the now free array slot */
            array_ent->buffer = buffer;
            array_ent->refcount = 0;

            t_thrd.storage_cxt.PrivateRefCountOverflowed++;

            return array_ent;
        }
    } else {
        if (!do_move) {
            return res;
        } else if (found && free_entry != NULL) {
            /* move buffer from hashtable into the free array slot
             *
             * fill array slot
             */
            free_entry->buffer = buffer;
            free_entry->refcount = res->refcount;

            /* delete from hashtable */
            (void)hash_search(t_thrd.storage_cxt.PrivateRefCountHash, (void *)&buffer, HASH_REMOVE, &found);
            Assert(found);
            Assert(t_thrd.storage_cxt.PrivateRefCountOverflowed > 0);
            t_thrd.storage_cxt.PrivateRefCountOverflowed--;

            return free_entry;
        } else {
            /*
             * Swap the entry in the hash table with the one in the array at the
             * current clock position.
             */
            PrivateRefCountEntry *array_ent = NULL;
            PrivateRefCountEntry *hash_ent = NULL;

            /* select victim slot */
            array_ent = &t_thrd.storage_cxt
                             .PrivateRefCountArray[t_thrd.storage_cxt.PrivateRefCountClock++ % REFCOUNT_ARRAY_ENTRIES];
            Assert(array_ent->buffer != InvalidBuffer);

            /* enter victim entry into the hashtable */
            hash_ent = (PrivateRefCountEntry *)hash_search(t_thrd.storage_cxt.PrivateRefCountHash,
                                                           (void *)&array_ent->buffer, HASH_ENTER, &found);
            Assert(!found);
            hash_ent->refcount = array_ent->refcount;

            /* fill now free array entry with previously searched entry */
            array_ent->buffer = res->buffer;
            array_ent->refcount = res->refcount;

            /* and remove the old entry */
            (void)hash_search(t_thrd.storage_cxt.PrivateRefCountHash, (void *)&array_ent->buffer, HASH_REMOVE, &found);
            Assert(found);

            /* PrivateRefCountOverflowed stays the same -1 + +1 = 0 */
            return array_ent;
        }
    }

    return NULL;
}

/* A combination of GetPrivateRefCountEntryFast & GetPrivateRefCountEntrySlow. */
PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool create, bool do_move)
{
    PrivateRefCountEntry *free_entry = NULL;
    PrivateRefCountEntry *ref = NULL;

    ref = GetPrivateRefCountEntryFast(buffer, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(buffer, create, do_move, free_entry);
    }

    return ref;
}

/*
 * Returns how many times the passed buffer is pinned by this backend.
 *
 * Only works for shared memory buffers!
 */
static int32 GetPrivateRefCount(Buffer buffer)
{
    PrivateRefCountEntry *ref = NULL;

    Assert(BufferIsValid(buffer));
    Assert(!BufferIsLocal(buffer));

    PrivateRefCountEntry *free_entry = NULL;
    ref = GetPrivateRefCountEntryFast(buffer, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(buffer, false, false, free_entry);
    }
    if (ref == NULL) {
        return 0;
    }
    return ref->refcount;
}

/*
 * Release resources used to track the reference count of a buffer which we no
 * longer have pinned and don't want to pin again immediately.
 */
void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref)
{
    Assert(ref->refcount == 0);

    if (ref >= &t_thrd.storage_cxt.PrivateRefCountArray[0] &&
        ref < &t_thrd.storage_cxt.PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES]) {
        ref->buffer = InvalidBuffer;
    } else {
        bool found = false;
        Buffer buffer = ref->buffer;
        (void)hash_search(t_thrd.storage_cxt.PrivateRefCountHash, (void *)&buffer, HASH_REMOVE, &found);
        Assert(found);
        Assert(t_thrd.storage_cxt.PrivateRefCountOverflowed > 0);
        t_thrd.storage_cxt.PrivateRefCountOverflowed--;
    }
}

static void BufferSync(int flags);
static uint32 WaitBufHdrUnlocked(BufferDesc* buf);
static void WaitIO(BufferDesc* buf);
static void TerminateBufferIO_common(BufferDesc* buf, bool clear_dirty, uint32 set_flag_bits);
void shared_buffer_write_error_callback(void* arg);
static BufferDesc* BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum, BlockNumber blockNum,
                               BufferAccessStrategy strategy, bool* foundPtr, const XLogPhyBlock *pblk);

static int rnode_comparator(const void* p1, const void* p2);

static int buffertag_comparator(const void* p1, const void* p2);

extern void PageRangeBackWrite(
    uint32 bufferIdx, int32 n, uint32 flags, SMgrRelation reln, int32* bufs_written, int32* bufs_reusable);
extern void PageListBackWrite(
    uint32* bufList, int32 n, uint32 flags, SMgrRelation reln, int32* bufs_written, int32* bufs_reusable);
#ifndef ENABLE_LITE_MODE
static volatile BufferDesc* PageListBufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
                                                BlockNumber blockNum, BufferAccessStrategy strategy, bool* foundPtr);
#endif
static bool ConditionalStartBufferIO(BufferDesc* buf, bool forInput);

/*
 * PrefetchBuffer -- initiate asynchronous read of a block of a relation
 *
 * This is named by analogy to ReadBuffer but doesn't actually allocate a
 * buffer.	Instead it tries to ensure that a future ReadBuffer for the given
 * block will not be delayed by the I/O.  Prefetching is optional.
 * No-op if prefetching isn't compiled in.
 */
void PrefetchBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum)
{
#if defined(USE_PREFETCH) && defined(USE_POSIX_FADVISE)
    Assert(RelationIsValid(reln));
    Assert(BlockNumberIsValid(blockNum));

    /* Open it at the smgr level if not already done */
    RelationOpenSmgr(reln);

    if (RelationUsesLocalBuffers(reln)) {
        /* see comments in ReadBufferExtended */
        if (RELATION_IS_OTHER_TEMP(reln)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot access temporary tables of other sessions")));
        }
    }

    BufferTag new_tag;          /* identity of requested block */
    uint32 new_hash;            /* hash value for newTag */
    LWLock *new_partition_lock; /* buffer partition lock for it */
    int buf_id;

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(new_tag, reln->rd_smgr->smgr_rnode.node, forkNum, blockNum);

    /* determine its hash code and partition lock ID */
    new_hash = BufTableHashCode(&new_tag);
    new_partition_lock = BufMappingPartitionLock(new_hash);

    /* see if the block is in the buffer pool already */
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    buf_id = BufTableLookup(&new_tag, new_hash);
    LWLockRelease(new_partition_lock);

    /* If not in buffers, initiate prefetch */
    if (buf_id < 0) {
        smgrprefetch(reln->rd_smgr, forkNum, blockNum);
    }

    /*
     * If the block *is* in buffers, we do nothing.  This is not really
     * ideal: the block might be just about to be evicted, which would be
     * stupid since we know we are going to need it soon.  But the only
     * easy answer is to bump the usage_count, which does not seem like a
     * great solution: when the caller does ultimately touch the block,
     * usage_count would get bumped again, resulting in too much
     * favoritism for blocks that are involved in a prefetch sequence. A
     * real fix would involve some additional per-buffer state, and it's
     * not clear that there's enough of a problem to justify that.
     */
#endif /* USE_PREFETCH && USE_POSIX_FADVISE */
}

/*
 * @Description: ConditionalStartBufferIO: conditionally begin and Asynchronous Prefetch or
 * WriteBack I/O on this buffer.
 *
 * Entry condition:
 * The buffer must be Pinned.
 *
 * In some scenarios there are race conditions in which multiple backends
 * could attempt the same I/O operation concurrently.  If another thread
 * has already started I/O on this buffer or is attempting it then return
 * false.
 *
 * If there is no active I/O on the buffer and the I/O has not already been
 * done, return true with the buffer marked I/O busy with the
 * io_in_progress_lock held.
 *
 * When this function returns true, the caller is free to perform the
 * I/O and terminate the operation.
 * @Param[IN] buf: buf desc
 * @Param[IN] forInput: true -- request thread; false--backend thread
 * @Return: true -- lock sucess; false-- lock failed
 * @See also:
 */
static bool ConditionalStartBufferIO(BufferDesc *buf, bool for_input)
{
    uint32 buf_state;

    /*
     * Grab the io_in_progress lock so that other processes can wait for
     * me to finish the I/O.  If we cannot acquire the lock it means
     * I/O we want to do is in progress on this buffer.
     */
    if (LWLockConditionalAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE) == true) {
        /* Got the lock */
        buf_state = LockBufHdr(buf);
        /*
         * If BM_IO_IN_PROGRESS and the lock isn't held,
         * it means the i/o was in progress and an error occured,
         * and some other thread should take care of this.
         */
        if (buf_state & BM_IO_IN_PROGRESS) {
            UnlockBufHdr(buf, buf_state);
            LWLockRelease(buf->io_in_progress_lock);
            return false;
        }
    } else {
        /*
         * Could not get the lock...
         * Another thread is currently attempting to read
         * or write this buffer.  We don't need to do our I/O.
         */
        return false;
    }

    /*
     * At this point, there is no I/O active on this buffer
     * We are holding the BufHdr lock and the io_in_progress_lock.
     */
    buf_state = pg_atomic_read_u32(&buf->state);
    if (for_input ? (buf_state & BM_VALID) : !(buf_state & BM_DIRTY)) {
        /* Another thread already did the I/O */
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);
        return false;
    }

    buf_state |= BM_IO_IN_PROGRESS;

    UnlockBufHdr(buf, buf_state);

    /*
     * Return holding the io_in_progress_lock,
     * The caller is expected to perform the actual I/O.
     */
    return true;
}

/*
 * @Description: PageListBufferAlloc--
 *
 *		This function determines whether the buffer is
 *		already in the buffer pool (busy or not).
 *		If the buffer is not in the buffer pool a
 *		a pinned buffer is returned ready for the caller to
 *		read into.
 *
 * 		Return value and *foundPtr:
 *
 *		1. If the buffer is in the buffer pool already (whether
 *		busy or not) return NULL and found = true.
 *
 *		2. If the buffer is not in the buffer pool a free or a clean
 *		buffer is found to recycle.
 *
 *		3. If a free or clean buffer cannot be found,
 *		return NULL and found = false.
 *
 *		4. If buffer is found, return the
 *		buf_desc for the buffer pinned and marked IO_IN_PROGRESS, ready for
 *		reading in the data, and found = false.
 *
 * "strategy" can be a buffer replacement strategy object, or NULL for
 * the default strategy.  The selected buffer's usage_count is advanced when
 * using the default strategy, but otherwise possibly not (see PinBuffer).
 *
 * The returned buffer is pinned and is already marked as holding the
 * desired page and the buffer is marked as IO_IN_PROGRESS.
 *
 * No locks are held either at entry or exit.  But of course the returned buffer
 * if any, is pinned and returned with the in_progress_lock held and IO_IN_PROGRESS set.
 * @Param[IN ] blockNum:block Num
 * @Param[IN ] forkNum: fork Num
 * @Param[IN/OUT] foundPtr: found or not
 * @Param[IN ] relpersistence: relation persistence type
 * @Param[IN ] smgr: smgr
 * @Param[IN ] strategy: buffer accsess strategy
 * @Return: buffer desc ptr
 * @See also:
 */
#ifndef ENABLE_LITE_MODE
static volatile BufferDesc *PageListBufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber fork_num,
                                                BlockNumber block_num, BufferAccessStrategy strategy, bool *found)
{
    int buf_id;
    BufferDesc *buf = NULL;
    BufferTag new_tag;                 /* identity of requested block */
    uint32 new_hash;                   /* hash value for newTag */
    LWLock *new_partition_lock = NULL; /* buffer partition lock for it */
    BufferTag old_tag;                 /* previous identity of buffer */
    uint32 old_hash;                   /* hash value for oldTag */
    LWLock *old_partition_lock = NULL; /* buffer partition lock for it */
    uint32 old_flags;
    uint32 buf_state;

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, fork_num, block_num);

    /* determine its hash code and partition lock ID */
    new_hash = BufTableHashCode(&new_tag);
    new_partition_lock = BufMappingPartitionLock(new_hash);

    /* see if the block is in the buffer pool already */
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    buf_id = BufTableLookup(&new_tag, new_hash);
    LWLockRelease(new_partition_lock);

    /*
     * If the buffer is already in the buffer pool
     * exit, there is no need to prefetch it.
     * Otherwise, there is more work to do.
     */
    if (buf_id >= 0) {
        *found = true;
        return NULL;
    } else {
        *found = false;
    }

    /*
     * Didn't find the block in the buffer pool.
     * Now we need to initialize a new buffer to do the prefetch.
     * There are no locks held so we may have to loop back
     * to get a buffer to update.
     *
     * Loop here in case we have to try another victim buffer
     */
    for (;;) {
        /*
         * Select a victim buffer.
         * The buffer is returned with its header spinlock still held!
         */
        pgstat_report_waitevent(WAIT_EVENT_BUF_STRATEGY_GET);
        buf = (BufferDesc*)StrategyGetBuffer(strategy, &buf_state);
        pgstat_report_waitevent(WAIT_EVENT_END);

        Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

        /* Must copy buffer flags while we still hold the spinlock */
        old_flags = buf_state & BUF_FLAG_MASK;

        /* Pin the buffer and then release the buffer spinlock */
        PinBuffer_Locked(buf);

        /*
         * At this point, the victim buffer is pinned
         * but no locks are held.
         *
         *
         * If StrategyGetBuffer() returns a dirty buffer,
         * it means we got it from the NULL strategy and
         * that the backwriter has not kept up with
         * writing out the buffers.
         *
         * We are not going to wait for for a dirty buffer to be
         * written and possibly an xlog update as well...
         */
        if (old_flags & (BM_DIRTY | BM_IS_META)) {
            /*
             * Cannot get a buffer to do the prefetch.
             */
            UnpinBuffer(buf, true);
            return NULL;
        }

        /*
         * Make the buffer ours, get an exclusive lock on both
         * the old and new mapping partitions.
         */
        if (old_flags & BM_TAG_VALID) {
            /*
             * Need to compute the old tag's hashcode and partition
             * lock ID.
             * Is it worth storing the hashcode in BufferDesc so
             * we need not recompute it here?  Probably not.
             */
            old_tag = buf->tag;
            old_hash = BufTableHashCode(&old_tag);
            old_partition_lock = BufMappingPartitionLock(old_hash);
            /* Must lock the lower-numbered partition first to avoid deadlocks. */
            if (old_partition_lock < new_partition_lock) {
                (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            } else if (old_partition_lock > new_partition_lock) {
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
                (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
            } else {
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            }
        } else {
            /* if it wasn't valid, we need only the new partition */
            (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            /* these just keep the compiler quiet about uninit variables */
            old_hash = 0;
            old_partition_lock = NULL;
        }

        /*
         * Try to make a hashtable entry for the buffer under its new
         * tag.  This could fail because while we were writing someone
         * else allocated another buffer for the same block we want to
         * read in. Note that we have not yet removed the hashtable
         * entry for the old tag.
         */
        buf_id = BufTableInsert(&new_tag, new_hash, buf->buf_id);
        if (buf_id >= 0) {
            /* Got a collision. Someone has already done what
             * we were about to do. We'll just handle this as
             * if it were found in the buffer pool in the
             * first place.  First, give up the buffer we were
             * planning to use. */
            UnpinBuffer(buf, true);

            /*
             * Can give up that buffer's mapping partition lock
             */
            if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock) {
                LWLockRelease(old_partition_lock);
            }

            /* Release the mapping lock */
            LWLockRelease(new_partition_lock);

            /*
             * There is no need to prefetch this buffer
             * It is already in the buffer pool.
             */
            *found = true;
            return NULL;
        }

        /*
         * At this point the buffer is pinned and the
         * partitions are locked
         *
         *
         * Need to lock the buffer header to change its tag.
         */
        buf_state = LockBufHdr(buf);

        /* Everything is fine, the buffer is ours, so break */
        old_flags = buf_state & BUF_FLAG_MASK;
        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY) && !(old_flags & BM_IS_META)) {
            break;
        }

        /*
         * Otherwise...
         * Somebody could have pinned or re-dirtied the buffer
         * while we were doing the I/O and making the new
         * hashtable entry.  If so, we can't recycle this buffer;
         * we must undo everything we've done and start
         * over with a new victim buffer.
         */
        UnlockBufHdr(buf, buf_state);
        BufTableDelete(&new_tag, new_hash);
        if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
        LWLockRelease(new_partition_lock);
        UnpinBuffer(buf, true);
    } /* loop back and try another victim */

    /*
     * Finally it is safe to rename the buffer!
     * We do what BufferAlloc() does to set the flags and count...
     */
    buf->tag = new_tag;
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT);
    if ((relpersistence == RELPERSISTENCE_PERMANENT) ||
        ((relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED)) {
        buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
    } else {
        buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
    }
    UnlockBufHdr(buf, buf_state);

    if (old_flags & BM_TAG_VALID) {
        BufTableDelete(&old_tag, old_hash);
        if (old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
    }
    LWLockRelease(new_partition_lock);

    /*
     * Buffer contents are currently invalid. Try to get the io_in_progress
     * lock.  If ConditionalStartBufferIO returns false, then
     * another thread managed to read it before we did,
     * so there's nothing left to do.
     */
    if (ConditionalStartBufferIO(buf, true) == true) {
        /* The buffer is pinned and marked I/O busy */
        *found = false;
        return buf;
    } else {
        UnpinBuffer(buf, true);
        *found = true;
        return NULL;
    }
}
#endif

/*
 * @Description: Prefetch sequential buffers from a database relation fork.
 * @Param[IN] blockNum:start block Num
 * @Param[IN] col:opt cloumn, not used now
 * @Param[IN] flags:opt flags, not used now
 * @Param[IN] forkNum: fork Num
 * @Param[IN] n: block count
 * @Param[IN] reln: relation
 * @See also:
 */
void PageRangePrefetch(Relation reln, ForkNumber fork_num, BlockNumber block_num, int32 n, uint32 flags = 0,
                       uint32 col = 0)
{
    BlockNumber *block_list = NULL;
    BlockNumber *block_ptr = NULL;
    BlockNumber block, end;

    /*
     * Allocate the block list.  It is only required
     * within the context of this function.
     */
    block_list = (BlockNumber *)palloc(sizeof(BlockNumber) * n);

    /*
     * Fill the blockList and call PageListPrefetch to process it.
     */
    for (block = block_num, end = block_num + n, block_ptr = block_list; block < end; block++) {
        *(block_ptr++) = block;
    }

    /*
     * Call PageListPrefetch to process the list
     */
    PageListPrefetch(reln, fork_num, block_list, n, flags, col);

    pfree(block_list);

    return;
}

/*
 * @Description: PageListPrefetch
 * The dispatch list of AioDispatchDesc_t structures is released
 * from this routine after the I/O is dispatched.
 * The  AioDispatchDesc_t structures allocated must hold
 * the I/O context to be used after the I/O is complete, so they
 * are deallocated from the completer thread.
 *
 * 1. This routine does not prefetch local buffers (this is probably
 * worth adding).
 *
 * MAX_PREFETCH_REQSIZE MAX_BACKWRITE_REQSIZ are the maximum number of
 * buffers sent to the I/O subsystem at once for Prefetch and
 * backwrite.  This is not the queue depth.
 * They cannot be too large because multiple threads can be
 * prefetching or backwriting.  The values must be changed in concert
 * with MAX_SIMUL_LWLOCKS, since each in-progress buffer requires a lock.
 * @Param[IN] blockList: block number list
 * @Param[IN] col: opt column,not used now
 * @Param[IN] flags: opt flags,  not used now
 * @Param[IN] forkNum: fork Num
 * @Param[IN] n: block count
 * @Param[IN] reln:relation
 * @See also:
 */
void PageListPrefetch(Relation reln, ForkNumber fork_num, BlockNumber *block_list, int32 n, uint32 flags = 0,
                      uint32 col = 0)
{
#ifndef ENABLE_LITE_MODE
    AioDispatchDesc_t **dis_list; /* AIO dispatch list */
    bool is_local_buf = false;    /* local buf flag */

    /* Exit without complaint, if there is no completer started yet */
    if (AioCompltrIsReady() == false) {
        return;
    }

    /* Open it at the smgr level if not already done */
    RelationOpenSmgr(reln);

    /*
     * Sorry, no prefetch on local bufs now.
     */
    is_local_buf = SmgrIsTemp(reln->rd_smgr);
    if (is_local_buf) {
        return;
    }

    /*
     * Allocate the dispatch list.  It is only needed
     * within the context of this function.
     */
    t_thrd.storage_cxt.InProgressAioDispatch =
        (AioDispatchDesc_t **)palloc(sizeof(AioDispatchDesc_t *) * MAX_PREFETCH_REQSIZ);
    dis_list = t_thrd.storage_cxt.InProgressAioDispatch;

    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioRead;

    /*
     * Reject attempts to write non-local temporary relations
     * Is this possible???
     */
    if (RELATION_IS_OTHER_TEMP(reln)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));
    }

    /*
     * For each page in the blockList...
     */
    for (int i = 0; i < n; i++) {
        BlockNumber block_num = block_list[i];
        Block buf_block;
        BufferDesc volatile *buf_desc = NULL;
        AioDispatchDesc_t *aio_desc = NULL;
        bool found = 0;
        bool is_extend = (block_num == P_NEW);
        t_thrd.storage_cxt.InProgressAioBuf = NULL;

        /*
         * Should not be extending the relation during prefetch
         * so skip the block.
         */
        if (is_extend) {
            continue;
        }

        /* Make sure we will have room to remember the buffer pin */
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

        /*
         * If the buffer was in the cache or in-progress
         * or could not be allocated, then skip it.
         * The found flag indicates whether it was found
         * in the cache.
         *
         * Once PageListBufferAlloc() returns, no locks are held.
         * The buffer is pinned and the buffer busy for i/o.
         */
        buf_desc = PageListBufferAlloc(reln->rd_smgr, reln->rd_rel->relpersistence, fork_num, block_num, NULL, &found);
        /* If we do not have a buffer to read into then skip this one */
        if (buf_desc == NULL) {
            continue;
        }
        t_thrd.storage_cxt.InProgressAioBuf = (BufferDesc *)buf_desc;

        /*
         * Allocate an iocb, we cannot use the internal allocator because this
         * buffer could live on past the timely death of this thread.
         * We expect that if the tread dies prematurely before dispatching
         * the i/o, the database will go down as well.
         */
        aio_desc = (AioDispatchDesc_t *)adio_share_alloc(sizeof(AioDispatchDesc_t));

        // PageListPrefetch: jeh need prefetch stats, allocated buffers, reads started?
        // Should use found for some statistics...
        // e.g. u_sess->instr_cxt.pg_buffer_usage->shared_blks_read++ for shared and temp as well ?
        // perhaps pagestat_info should have a prefetch counter
        // An actual read will update the counters so we don't want to update
        // the hit count, but do we need something else to keep the pages in memory
        // when they were just prefetched? or to allow them to be reused sooner...
        /*
         * At this point, a buffer has been allocated but its contents
         * are NOT valid.  For a shared buffer the IO_IN_PROGRESS
         * flag is set.
         */
        Assert(!(pg_atomic_read_u32(&buf_desc->state) & BM_VALID)); /* spinlock not needed */

        buf_block = is_local_buf ? LocalBufHdrGetBlock(buf_desc) : BufHdrGetBlock(buf_desc);

        /* iocb filled in later */
        aio_desc->aiocb.data = 0;
        aio_desc->aiocb.aio_fildes = 0;
        aio_desc->aiocb.aio_lio_opcode = 0;
        aio_desc->aiocb.u.c.buf = 0;
        aio_desc->aiocb.u.c.nbytes = 0;
        aio_desc->aiocb.u.c.offset = 0;

        /* AIO block descriptor filled here */
        Assert(buf_desc->tag.forkNum == MAIN_FORKNUM);
        Assert(reln->rd_smgr == smgropen(((BufferDesc *)buf_desc)->tag.rnode, InvalidBackendId,
                                         GetColumnNum(((BufferDesc *)buf_desc)->tag.forkNum)));
        aio_desc->blockDesc.smgrReln = reln->rd_smgr;
        aio_desc->blockDesc.forkNum = buf_desc->tag.forkNum;
        aio_desc->blockDesc.blockNum = buf_desc->tag.blockNum;
        aio_desc->blockDesc.buffer = buf_block;
        aio_desc->blockDesc.blockSize = BLCKSZ;
        aio_desc->blockDesc.reqType = PageListPrefetchType;
        aio_desc->blockDesc.bufHdr = (BufferDesc *)buf_desc;
        aio_desc->blockDesc.descType = AioRead;

        dis_list[t_thrd.storage_cxt.InProgressAioDispatchCount++] = aio_desc;
        t_thrd.storage_cxt.InProgressAioBuf = NULL;

        /*
         * If the buffer is full, dispatch the I/O in the dList, if any
         */
        if (t_thrd.storage_cxt.InProgressAioDispatchCount >= MAX_PREFETCH_REQSIZ) {
            HOLD_INTERRUPTS();
            smgrasyncread(reln->rd_smgr, fork_num, dis_list, t_thrd.storage_cxt.InProgressAioDispatchCount);
            t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
            RESUME_INTERRUPTS();
        }
    }

    // Send any remaining buffers
    if (t_thrd.storage_cxt.InProgressAioDispatchCount > 0) {
        HOLD_INTERRUPTS();
        smgrasyncread(reln->rd_smgr, fork_num, dis_list, t_thrd.storage_cxt.InProgressAioDispatchCount);
        t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
        RESUME_INTERRUPTS();
    }

    pfree(dis_list);
    t_thrd.storage_cxt.InProgressAioDispatch = NULL;
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;
#endif
}

/*
 * @Description: aio clean up I/O status for prefetch
 * @See also:
 */
void PageListPrefetchAbort()
{
    int count = t_thrd.storage_cxt.InProgressAioDispatchCount;
    int already_submit_count = u_sess->storage_cxt.AsyncSubmitIOCount;
    AioDispatchDesc_t **dis_list = t_thrd.storage_cxt.InProgressAioDispatch;

    Assert(t_thrd.storage_cxt.InProgressAioType == AioRead);

    if (t_thrd.storage_cxt.InProgressAioBuf != NULL) {
        ereport(LOG, (errmsg("TerminateBufferIO_common: set bud_id(%d) IO_ERROR,",
                             t_thrd.storage_cxt.InProgressAioBuf->buf_id)));
        TerminateBufferIO_common(t_thrd.storage_cxt.InProgressAioBuf, false, BM_IO_ERROR);
        t_thrd.storage_cxt.InProgressAioBuf = NULL;
    }

    if (t_thrd.storage_cxt.InProgressAioDispatchCount == 0) {
        return;
    }
    Assert(t_thrd.storage_cxt.InProgressAioDispatch != NULL);

    ereport(LOG, (errmsg("aio prefetch: catch error aio dispatch count(%d)", count)));
    for (int i = already_submit_count; i < count; i++) {
        if (dis_list[i] == NULL) {
            continue;
        }
        BufferDesc *buf_desc = dis_list[i]->blockDesc.bufHdr;
        if (buf_desc != NULL) {
            TerminateBufferIO_common(buf_desc, false, BM_IO_ERROR);
            ereport(LOG, (errmsg("TerminateBufferIO_common: set bud_id(%d) IO_ERROR,", buf_desc->buf_id)));
            if (!LWLockHeldByMe(buf_desc->io_in_progress_lock)) {
                LWLockOwn(buf_desc->io_in_progress_lock);
                LWLockRelease(buf_desc->io_in_progress_lock);
                AsyncCompltrUnpinBuffer((volatile void *)(buf_desc));
                ereport(LOG, (errmsg("LWLockRelease: bud_id(%d) release in_progress_lock and unpin buffer,",
                                     buf_desc->buf_id)));
            }
            dis_list[i]->blockDesc.bufHdr = NULL;
        }

        adio_share_free(dis_list[i]);
        dis_list[i] = NULL;
    }
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioDispatch = NULL;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;
    u_sess->storage_cxt.AsyncSubmitIOCount = 0;
}

/*
 * @Description: Write sequential buffers from a database relation fork.
 * @Param[IN] bufferIdx: starting buffer index
 * @Param[IN] bufs_reusable: opt reusable count returned
 * @Param[IN] bufs_written: opt written count returned
 * @Param[IN] flags: opt flags, not used now
 * @Param[IN] n: number of bufs to scan
 * @Param[IN] smgrReln: smgr relation
 * @See also:
 */
void PageRangeBackWrite(uint32 buffer_idx, int32 n, uint32 flags = 0, SMgrRelation smgr_reln = NULL,
                        int32 *bufs_written = NULL, int32 *bufs_reusable = NULL)
{
    uint32 *buf_list = NULL;
    uint32 *buf_ptr = NULL;
    int bi, ct;

    /*
     * Allocate the block list.  It is only required
     * within the context of this function.
     */
    buf_list = (uint32 *)palloc(sizeof(BlockNumber) * n);

    /*
     * Fill the blockList and call PageListPrefetch to process it.
     * Note that the shared buffers are in an array of NBuffer
     * treated as a circular list.
     */
    buf_ptr = buf_list;
    bi = buffer_idx;
    for (ct = 0; ct < n; ct++) {
        if (bi >= g_instance.attr.attr_storage.NBuffers) {
            bi = 0;
        }
        *buf_ptr = bi;
        bi++;
        buf_ptr++;
    }

    /* Identify that the bufList was supplied by this function */
    flags |= PAGERANGE_BACKWRITE;

    /*
     * Process the list
     */
    PageListBackWrite(buf_list, n, flags, smgr_reln, bufs_written, bufs_reusable);

    pfree(buf_list);
    return;
}

// max share memory is INT_MAX / 2
#define INVALID_SHARE_BUFFER_ID (uint32)0xFFFFFFFF

/*
 * @Description:  PageListBackWrite
 * The dispatch list of AioDispatchDesc_t structures is released
 * from this routine after the I/O is dispatched.
 * The  AioDispatchDesc_t structures allocated must hold
 * the I/O context to be used after the I/O is complete, so they
 * are deallocated from the completer thread.
 * @Param[IN] bufList: buffer list
 * @Param[IN] bufs_reusable: opt reusable count returned
 * @Param[IN] bufs_written: opt written count returned
 * @Param[IN] flags: opt flags
 * @Param[IN] nBufs: buffer count
 * @Param[IN] use_smgrReln: relation
 * @See also:
 */
void PageListBackWrite(uint32 *buf_list, int32 nbufs, uint32 flags = 0, SMgrRelation use_smgr_reln = NULL,
                       int32 *bufs_written = NULL, int32 *bufs_reusable = NULL)
{
    AioDispatchDesc_t **dis_list; /* AIO dispatch list */
    int bufs_written_local = 0;
    int bufs_reusable_local = 0;
    bool checkpoint_backwrite = (flags & CHECKPOINT_BACKWRITE);
    bool reorder_writes = (flags & (LAZY_BACKWRITE | CHECKPOINT_BACKWRITE));
    bool scratch_buflist = (flags & PAGERANGE_BACKWRITE);
    bool strategy_writes = (flags & STRATEGY_BACKWRITE);
    bool new_pass = false;
    int bufs_blocked = 0;

    ErrorContextCallback errcontext;

    if (reorder_writes && !scratch_buflist) {
        ereport(PANIC, (errmsg("Require scratch buflist to reorder writes.")));
    }

    /*
     * Allocate the dispatch list.  It is only needed
     * within the context of this function. No more than
     * MAX_PREFETCH_REQSIZ buffers are sent at once.
     */
    t_thrd.storage_cxt.InProgressAioDispatch =
        (AioDispatchDesc_t **)palloc(sizeof(AioDispatchDesc_t *) * MAX_BACKWRITE_REQSIZ);
    dis_list = t_thrd.storage_cxt.InProgressAioDispatch;

    /* Start at the beginning of the list */
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioWrite;

    do {
        bufs_blocked = 0;

        /*
         * Make one pass through the bufList
         */
        for (int i = 0; i < nbufs; i++) {
            AioDispatchDesc_t *aioDescp = NULL;
            XLogRecPtr recptr;
            SMgrRelation smgrReln;
            BufferDesc *bufHdr = NULL;
            uint32 buf_state;

            t_thrd.storage_cxt.InProgressAioBuf = NULL;

            /* Skip written buffers */
            if (buf_list[i] == INVALID_SHARE_BUFFER_ID) {
                continue;
            }
            /* ckpt/bgwriter pass buf_id, strategy pass Buffer, Buffer = buf_id +1, see AddBufferToRing() */
            if (strategy_writes) {
                /* Skip unused buffers */
                if (buf_list[i] == 0) {
                    continue;
                }
                /* Lookup the bufHdr for the given block */
                bufHdr = GetBufferDescriptor(buf_list[i] - 1);
            } else {
                /* Lookup the bufHdr for the given block */
                bufHdr = GetBufferDescriptor(buf_list[i]);
            }

            /* Make sure we will have room to remember the buffer pin */
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

            /*
             * Check whether buffer needs writing.
             *
             * We can make this check without taking the buffer content
             * lock so long as we mark pages dirty in access methods
             * before logging changes with XLogInsert(): if someone
             * marks the buffer dirty just after our check we don't worry
             * because our checkpoint.redo points before log record for
             * upcoming changes and so we are not required to write
             * such dirty buffer.
             */
            buf_state = LockBufHdr(bufHdr);
            /*
             * For checkpoints, only sync buffers that are marked
             * BM_CHECKPOINT_NEEDED. Also avoid locking the buffer
             * header for buffers that do not have to be synced.
             *
             * It is unnecessary to hold the buffer lock to
             * check whether BM_CHECKPOINT_NEEDED is set:
             * 	1. We are only checking the one bit.
             * 	2. This is just an optimization, if we find
             * 	   BM_CHECKPOINT_NEEDED is set we will acquire the
             * 	   lock then to really decide whether the write
             * 	   is necessary.
             *	3. In the unlikely event that another thread writes
             *	   and replaces the buffer with another dirty one,
             *	   before we get the lock and check it again,
             *	   we could do an unnecessary write, but that is
             *	   fine, given it is very unlikely.
             */
            if (checkpoint_backwrite && !(buf_state & BM_CHECKPOINT_NEEDED)) {
                UnlockBufHdr(bufHdr, buf_state);
                if (reorder_writes) {
                    buf_list[i] = INVALID_SHARE_BUFFER_ID;
                }
                continue;
            }

            /*
             * Do not process bogus buffers
             */
            if (!(buf_state & BM_VALID)) {
                UnlockBufHdr(bufHdr, buf_state);
                if (reorder_writes) {
                    buf_list[i] = INVALID_SHARE_BUFFER_ID;
                }
                continue;
            }

            /*
             * Count the number of buffers that were *not* recently used
             * these will be written out regardless of the caller.
             */
            if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 && BUF_STATE_GET_USAGECOUNT(buf_state) == 0) {
                ++bufs_reusable_local;
            } else if (!checkpoint_backwrite) {
                /*
                 * Skip recently used buffers except when performing
                 * a checkpoint.
                 */
                UnlockBufHdr(bufHdr, buf_state);
                if (reorder_writes) {
                    buf_list[i] = INVALID_SHARE_BUFFER_ID;
                }
                continue;
            }

            /*
             * Clean buffers do not need to be written out
             */
            if (!(buf_state & BM_VALID) || !(buf_state & BM_DIRTY)) {
                /* It's clean, so nothing to do */
                Assert(!(buf_state & BM_CHECKPOINT_NEEDED));
                UnlockBufHdr(bufHdr, buf_state);
                if (reorder_writes) {
                    buf_list[i] = INVALID_SHARE_BUFFER_ID;
                }
                continue;
            }

            /*
             * Pin buffer.
             */
            PinBuffer_Locked(bufHdr);

            /* Share lock the buffer...
             *
             * For sequential writes, always acquire the content_lock as usual,
             * acquiring the lock can block briefly but a deadlock
             * cannot occur.  Issuing sequential requests together, in-order
             * has disk I/O performance advantages.
             *
             * For reordering writes (called from PageRangeBackWrite()), the first
             * pass through the list any buffers that can be accessed without
             * waiting are started.  The assumption here is that most of the
             * buffers are uncontended and this will start a lot of I/O,
             * while we wait here for the others to get unblocked.
             *
             * On subsequent passes, this thread may sleep waiting for the
             * content_lock on the first remaining buffer.  This guarantees
             * that at least one buffer can be started on each new_pass, and
             * hopefully many more become unblocked at the same time.
             * For highly contended buffers this can provide some time for
             * writes blocking forward progress on the list to finish
             * making it possible to start many requests even on subsequent
             * passes. There is no guarantee of this of course.
             *
             * This routine never blocks while holding any buffers for submittal.
             * it is possible to deadlock waiting on a buffer that is held by
             * another thread that itself is waiting for lock on a buffer already
             * held by this thread. The downside is that if there are buffers
             * that are highly contended but not held too long, they may end up
             * being submitted on their own.  We really need to be able to
             * timeout a lightweight lock request.
             */
            if (reorder_writes && !new_pass) {
                if (!LWLockConditionalAcquire(bufHdr->content_lock, LW_SHARED)) {
                    UnpinBuffer(bufHdr, true);
                    bufs_blocked++;
                    continue;
                }
            } else {
                (void)LWLockAcquire(bufHdr->content_lock, LW_SHARED);
                new_pass = false;
            }

            /*
             * Acquire the buffer's io_in_progress lock.
             * If ConditionalStartBufferIO returns false, then another
             * thread is flushing the buffer before we could, so we
             * can skip this one.
             *
             * We do not have to worry about the flags on the skipped buffer
             * because the other thread will set/clear them as necessary
             * once the buffer is written.
             */
            if (ConditionalStartBufferIO(bufHdr, false) == false) {
                LWLockRelease(bufHdr->content_lock);
                UnpinBuffer(bufHdr, true);
                if (reorder_writes) {
                    buf_list[i] = INVALID_SHARE_BUFFER_ID;
                }
                continue;
            }
            t_thrd.storage_cxt.InProgressAioBuf = bufHdr;

            /*
             * At this point, the buffer is pinned,
             * the content_lock is held in shared mode,
             * and the io_in_progress_lock is held.
             * The buffer header is not locked.
             *
             * Setup error traceback support for ereport()
             */
            errcontext.callback = shared_buffer_write_error_callback;
            errcontext.arg = (void *)bufHdr;
            errcontext.previous = t_thrd.log_cxt.error_context_stack;
            t_thrd.log_cxt.error_context_stack = &errcontext;

            /* PageListBackWrite: jeh smgropen blocks */
            /* If a relation is provided, use it.
             * otherwise look up the reln for each buffer.
             */
            smgrReln = smgropen(bufHdr->tag.rnode, InvalidBackendId, GetColumnNum(bufHdr->tag.forkNum));

            TRACE_POSTGRESQL_BUFFER_FLUSH_START(bufHdr->tag.forkNum, bufHdr->tag.blockNum,
                                                smgrReln->smgr_rnode.node.spcNode, smgrReln->smgr_rnode.node.dbNode,
                                                smgrReln->smgr_rnode.node.relNode);

            Assert(smgrReln->smgr_rnode.node.spcNode == bufHdr->tag.rnode.spcNode);
            Assert(smgrReln->smgr_rnode.node.dbNode == bufHdr->tag.rnode.dbNode);
            Assert(smgrReln->smgr_rnode.node.relNode == bufHdr->tag.rnode.relNode);
            Assert(smgrReln->smgr_rnode.node.bucketNode == bufHdr->tag.rnode.bucketNode);
            Assert(smgrReln->smgr_rnode.node.opt == bufHdr->tag.rnode.opt);
            
            /* PageListBackWrite: jeh XLogFlush blocking? */
            /*
             * Force XLOG flush up to buffer's LSN.  This implements the basic WAL
             * rule that log updates must hit disk before any of the data-file
             * changes they describe do.
             */
            recptr = BufferGetLSN(bufHdr);
            XLogWaitFlush(recptr);

            /*
             * Now it's safe to write the buffer. The io_in_progress
             * lock was held while waiting for the log update, to ensure
             * the buffer was not changed by any other thread.
             *
             *
             * Clear the BM_JUST_DIRTIED flag used
             * to check whether block content changes while flushing.
             */
            buf_state = LockBufHdr(bufHdr);
            buf_state &= ~BM_JUST_DIRTIED;
            UnlockBufHdr(bufHdr, buf_state);

            /*
             * Allocate an iocb, fill it in, and write the addr in the
             * dList array.
             */
            aioDescp = (AioDispatchDesc_t *)adio_share_alloc(sizeof(AioDispatchDesc_t));

            if (reorder_writes) {
                buf_list[i] = INVALID_SHARE_BUFFER_ID;
            }

            /* iocb filled in later */
            aioDescp->aiocb.data = 0;
            aioDescp->aiocb.aio_fildes = 0;
            aioDescp->aiocb.aio_lio_opcode = 0;
            aioDescp->aiocb.u.c.buf = 0;
            aioDescp->aiocb.u.c.nbytes = 0;
            aioDescp->aiocb.u.c.offset = 0;

            /* AIO block descriptor filled here */
            aioDescp->blockDesc.smgrReln = smgrReln;
            aioDescp->blockDesc.forkNum = bufHdr->tag.forkNum;
            aioDescp->blockDesc.blockNum = bufHdr->tag.blockNum;
            aioDescp->blockDesc.buffer = (char *)BufHdrGetBlock(bufHdr);
            aioDescp->blockDesc.blockSize = BLCKSZ;
            aioDescp->blockDesc.reqType = PageListBackWriteType;
            aioDescp->blockDesc.bufHdr = bufHdr;
            aioDescp->blockDesc.descType = AioWrite;

            dis_list[t_thrd.storage_cxt.InProgressAioDispatchCount++] = aioDescp;
            t_thrd.storage_cxt.InProgressAioBuf = NULL;

            /*
             * Submit the I/O if the dispatch list is full and refill the dlist.
             */
            if (t_thrd.storage_cxt.InProgressAioDispatchCount >= MAX_BACKWRITE_REQSIZ) {
                HOLD_INTERRUPTS();
                /*
                 * just get the info from the first one
                 */
                smgrasyncwrite(dis_list[0]->blockDesc.smgrReln, dis_list[0]->blockDesc.forkNum, dis_list,
                               t_thrd.storage_cxt.InProgressAioDispatchCount);

                /*
                 * Update the count of buffer writes initiated
                 * the u_sess->instr_cxt.pg_buffer_usage->shared_blks_written counter will
                 * be updated after the I/O is completed.
                 */
                bufs_written_local += t_thrd.storage_cxt.InProgressAioDispatchCount;

                /* Reuse the dList */
                t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
                RESUME_INTERRUPTS();
            }

            /* Pop the error context stack */
            t_thrd.log_cxt.error_context_stack = errcontext.previous;
        } /* for each buf in nBufs */

        /* At this point, some reordered writes might have been blocked.
         * If so, bufsBlocked indicates how many.  If none were blocked
         * bufsBlocked is zero, and this outer loop will exit.
         * There can be buffers on the dispatch list.  The dn value
         * indicates the number of buffers on the list.
         *
         * Before starting a new pass, for reorder_writes send any
         * buffers on the dispatch list.  Completing these writes
         * will cause the locks on these to be freed, possibly allowing
         * other buffers in the cache to be unblocked.
         *
         * The new_pass flag is set to true after some I/O is done
         * to indicate that it is safe to block waiting to acquire a lock.
         * because this thread will at most wait for just one buffer.
         *
         * Any remaining buffers in the dispatch list are always
         * sent at the end of the last pass.
         */
        if ((reorder_writes || bufs_blocked == 0) && t_thrd.storage_cxt.InProgressAioDispatchCount) {
            HOLD_INTERRUPTS();
            smgrasyncwrite(dis_list[0]->blockDesc.smgrReln, dis_list[0]->blockDesc.forkNum, dis_list,
                           t_thrd.storage_cxt.InProgressAioDispatchCount);

            /*
             * Update the count of buffer writes initiated
             * the u_sess->instr_cxt.pg_buffer_usage->shared_blks_written counter will
             * be updated after the I/O is completed.
             */
            bufs_written_local += t_thrd.storage_cxt.InProgressAioDispatchCount;

            /* Reuse the dList */
            t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
            RESUME_INTERRUPTS();
        } /* if... */

        new_pass = true;
    } while (bufs_blocked > 0);

    /* Return the requested statistics */
    if (bufs_reusable != NULL) {
        *bufs_reusable = bufs_reusable_local;
    }
    if (bufs_written != NULL) {
        *bufs_written = bufs_written_local;
    }
    pfree(dis_list);
    t_thrd.storage_cxt.InProgressAioDispatch = NULL;
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;
}

/*
 * @Description: aio clean up I/O status for backwrite
 * @See also:
 * notes: if checkpoint failed when shutdown, the lwlock may not get because lwlocks release earily
 * so FATAL the thread, but it rare happend
 */
void PageListBackWriteAbort()
{
    int count = t_thrd.storage_cxt.InProgressAioDispatchCount;
    int already_submit_count = u_sess->storage_cxt.AsyncSubmitIOCount;
    AioDispatchDesc_t **dis_list = t_thrd.storage_cxt.InProgressAioDispatch;

    Assert(t_thrd.storage_cxt.InProgressAioType == AioWrite || t_thrd.storage_cxt.InProgressAioType == AioVacummFull);

    if (t_thrd.storage_cxt.InProgressAioBuf != NULL) {
        ereport(LOG, (errmsg("TerminateBufferIO_common: set bud_id(%d) IO_ERROR,",
                             t_thrd.storage_cxt.InProgressAioBuf->buf_id)));
        TerminateBufferIO_common(t_thrd.storage_cxt.InProgressAioBuf, false, 0);
        t_thrd.storage_cxt.InProgressAioBuf = NULL;
    }

    if (t_thrd.storage_cxt.InProgressAioDispatchCount == 0) {
        return;
    }
    Assert(t_thrd.storage_cxt.InProgressAioDispatch != NULL);

    ereport(LOG, (errmsg("aio back write: catch error aio dispatch count(%d)", count)));
    Assert(already_submit_count <= count);
    for (int i = already_submit_count; i < count; i++) {
        if (dis_list[i] == NULL) {
            continue;
        }
        BufferDesc *buf_desc = dis_list[i]->blockDesc.bufHdr;
        if (buf_desc != NULL && dis_list[i]->blockDesc.descType == AioWrite) {
            TerminateBufferIO_common(buf_desc, false, 0);
            ereport(LOG, (errmsg("TerminateBufferIO_common: set bud_id(%d) IO_ERROR,", buf_desc->buf_id)));
            if (!LWLockHeldByMe(buf_desc->content_lock)) {
                LWLockOwn(buf_desc->content_lock);
                LWLockRelease(buf_desc->content_lock);
                ereport(LOG, (errmsg("LWLockRelease: bud_id(%d) release content_lock,", buf_desc->buf_id)));
            }
            if (!LWLockHeldByMe(buf_desc->io_in_progress_lock)) {
                LWLockOwn(buf_desc->io_in_progress_lock);
                LWLockRelease(buf_desc->io_in_progress_lock);
                AsyncCompltrUnpinBuffer((volatile void *)(buf_desc));
                ereport(LOG, (errmsg("LWLockRelease: bud_id(%d) release in_progress_lock and unpin buffer,",
                                     buf_desc->buf_id)));
            }
            dis_list[i]->blockDesc.bufHdr = NULL;
        }

        adio_share_free(dis_list[i]);
        dis_list[i] = NULL;
    }
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioDispatch = NULL;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;
    u_sess->storage_cxt.AsyncSubmitIOCount = 0;
}

/*
 * @Description: AsyncUnpinBuffer
 * This function calls UnpinBuffer,
 * providing the bufHdr via an opaque BufferDesc pointer.
 *
 * It is meant to be used in the AIO lower layers to release
 * the pin and forget the buffer at the moment the I/O is sent.
 * @Param[IN] bufHdr: buffer hander
 * @Param[IN] forgetBuffer: clean resource owner
 * @See also:
 */
void AsyncUnpinBuffer(volatile void *buf_desc, bool forget_buffer)
{
    BufferDesc *buf = (BufferDesc *)buf_desc;

    UnpinBuffer(buf, forget_buffer);
}

/*
 * @Description:  AsyncCompltrPinBuffer
 * Pin the buffer for the ADIO completer. Uses only the
 * shared buffer reference count.
 * @Param[IN] bufHdr:buffer hander
 * @See also:
 */
void AsyncCompltrPinBuffer(volatile void *buf_desc)
{
    uint32 buf_state;
    BufferDesc *buf = (BufferDesc *)buf_desc;

    buf_state = LockBufHdr(buf);

    /* Increment the shared reference count */
    buf_state += BUF_REFCOUNT_ONE;

    UnlockBufHdr(buf, buf_state);
}

/*
 * @Description: AsyncCompltrUnpinBuffer
 * Unpin the buffer for the ADIO completer, then wake any waiters.
 * @Param[IN] bufHdr: buffer hander
 * @See also:
 */
void AsyncCompltrUnpinBuffer(volatile void *buf_desc)
{
    uint32 buf_state;
    BufferDesc *buf = (BufferDesc *)buf_desc;

    buf_state = LockBufHdr(buf);

    /* Decrement the shared reference count */
    Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
    buf_state -= BUF_REFCOUNT_ONE;

    /* Support the function LockBufferForCleanup() */
    if ((buf_state & BM_PIN_COUNT_WAITER) && BUF_STATE_GET_REFCOUNT(buf_state) == 1) {
        /* we just released the last pin other than the waiter's */
        ThreadId wait_backend_pid = buf->wait_backend_pid;

        buf_state &= ~BM_PIN_COUNT_WAITER;
        UnlockBufHdr(buf, buf_state);
        ProcSendSignal(wait_backend_pid);
    } else
        UnlockBufHdr(buf, buf_state);
}

/*
 * AsyncCompltrUnpinBuffer
 *
 *
 * ReadBuffer -- a shorthand for ReadBufferExtended, for reading from main
 *		fork with RBM_NORMAL mode and default strategy.
 */
Buffer ReadBuffer(Relation reln, BlockNumber block_num)
{
    return ReadBufferExtended(reln, MAIN_FORKNUM, block_num, RBM_NORMAL, NULL);
}

/*
 * ReadBufferExtended -- returns a buffer containing the requested
 *		block of the requested relation.  If the blknum
 *		requested is P_NEW, extend the relation file and
 *		allocate a new block.  (Caller is responsible for
 *		ensuring that only one backend tries to extend a
 *		relation at the same time!)
 *
 * Returns: the buffer number for the buffer containing
 *		the block read.  The returned buffer has been pinned.
 *		Does not return on error --- elog's instead.
 *
 * Assume when this function is called, that reln has been opened already.
 *
 * In RBM_NORMAL mode, the page is read from disk, and the page header is
 * validated.  An error is thrown if the page header is not valid.	(But
 * note that an all-zero page is considered "valid"; see PageIsVerified().)
 *
 * RBM_ZERO_ON_ERROR is like the normal mode, but if the page header is not
 * valid, the page is zeroed instead of throwing an error. This is intended
 * for non-critical data, where the caller is prepared to repair errors.
 *
 * In RBM_ZERO_AND_LOCK mode, if the page isn't in buffer cache already,
 * it's filled with zeros instead of reading it from disk. Useful when the caller
 * is going to fill the page from scratch, since this saves I/O and avoids
 * unnecessary failure if the page-on-disk has corrupt page headers.
 * The page is returned locked to ensure that the caller has a chance to
 * initialize the page before it's made visible to others.
 * Caution: do not use this mode to read a page that is beyond the relation's
 * current physical EOF; that is likely to cause problems in md.c when
 * the page is modified and written out. P_NEW is OK, though.
 *
 * RBM_ZERO_AND_CLEANUP_LOCK is the same as RBM_ZERO_AND_LOCK, but
 * acquires a cleanup-strength lock on the page.
 *
 * RBM_NORMAL_NO_LOG mode is treated the same as RBM_NORMAL here.
 *
 * RBM_FOR_REMOTE is like the normal mode, but not remote read again when  PageIsVerified failed.
 *
 * If strategy is not NULL, a nondefault buffer access strategy is used.
 * See buffer/README for details.
 */
Buffer ReadBufferExtended(Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode,
                          BufferAccessStrategy strategy)
{
    bool hit = false;
    Buffer buf;

    if (block_num == P_NEW) {
        STORAGE_SPACE_OPERATION(reln, BLCKSZ);
    }

    /* Open it at the smgr level if not already done */
    RelationOpenSmgr(reln);

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if (RELATION_IS_OTHER_TEMP(reln) && fork_num <= INIT_FORKNUM)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));

    /*
     * Read the buffer, and update pgstat counters to reflect a cache hit or
     * miss.
     */
    pgstat_count_buffer_read(reln);
    pgstatCountBlocksFetched4SessionLevel();

    if (RelationisEncryptEnable(reln)) {
        reln->rd_smgr->encrypt = true;
    }
    buf = ReadBuffer_common(reln->rd_smgr, reln->rd_rel->relpersistence, fork_num,
                            block_num, mode, strategy, &hit, NULL);
    if (hit) {
        pgstat_count_buffer_hit(reln);
    }
    return buf;
}

/*
 * ReadBufferWithoutRelcache -- like ReadBufferExtended, but doesn't require
 *		a relcache entry for the relation.
 *
 * NB: At present, this function may only be used on permanent relations, which
 * is OK, because we only use it during XLOG replay and segment-page copy relation data.  
 * If in the future we want to use it on temporary or unlogged relations, we could pass 
 * additional parameters.
 */
Buffer ReadBufferWithoutRelcache(const RelFileNode &rnode, ForkNumber fork_num, BlockNumber block_num,
                                 ReadBufferMode mode, BufferAccessStrategy strategy, const XLogPhyBlock *pblk)
{
    bool hit = false;

    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

    return ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, fork_num, block_num, mode, strategy, &hit, pblk);
}

Buffer ReadUndoBufferWithoutRelcache(const RelFileNode& rnode, ForkNumber forkNum, 
    BlockNumber blockNum, ReadBufferMode mode, BufferAccessStrategy strategy,
    char relpersistence)
{
    bool hit = false;

    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

    return ReadBuffer_common(smgr, relpersistence, forkNum, blockNum, mode, strategy, &hit, NULL);
}

/*
 * ReadBufferForRemote -- like ReadBufferExtended, but doesn't require
 *		a relcache entry for the relation.
 *
 * NB: At present, this function may only be used on permanent relations, which
 * is OK, because we only use it during XLOG replay.  If in the future we
 * want to use it on temporary or unlogged relations, we could pass additional
 * parameters.
 */
Buffer ReadBufferForRemote(const RelFileNode &rnode, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode,
                           BufferAccessStrategy strategy, bool *hit, const XLogPhyBlock *pblk)
{
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

    if (unlikely(fork_num >= smgr->md_fdarray_size || fork_num < 0)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("invalid forkNum %d, should be less than %d", fork_num, smgr->md_fdarray_size)));
    }


    return ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, fork_num, block_num, mode, strategy, hit, pblk);
}

/*
 * ReadBuffer_common -- common logic for all ReadBuffer variants
 *
 * *hit is set to true if the request was satisfied from shared buffer cache.
 */
Buffer ReadBuffer_common_for_localbuf(RelFileNode rnode, char relpersistence, ForkNumber forkNum, BlockNumber blockNum,
                                      ReadBufferMode mode, BufferAccessStrategy strategy, bool *hit)
{
    BufferDesc *bufHdr = NULL;
    Block bufBlock;
    bool found = false;
    bool isExtend = false;
    bool need_reapir = false;

    *hit = false;
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    /* Make sure we will have room to remember the buffer pin */
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    isExtend = (blockNum == P_NEW);

    /* Substitute proper block number if caller asked for P_NEW */
    if (isExtend) {
        blockNum = smgrnblocks(smgr, forkNum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* When the parallel redo is enabled, there may be a scenario where
     * the index is replayed before the page replayed. For single-mode,
     * readable standby feature, Operators related to index scan access
     * the index first, then access the table, and you will find that
     * the tid or the heap(page) does not exist. Because the transaction
     * was not originally committed, the tid or the heap(page) should not
     * be visible. So accessing the non-existent heap tuple by the tid
     * should return that the tuple does not exist without error reporting.
     */
    else if (RecoveryInProgress()) {
        BlockNumber totalBlkNum = smgrnblocks_cached(smgr, forkNum);

        /* Update cached blocks */
        if (totalBlkNum == InvalidBlockNumber || blockNum >= totalBlkNum) {
            totalBlkNum = smgrnblocks(smgr, forkNum);
        }

        if (blockNum >= totalBlkNum) {
            return InvalidBuffer;
        }
    }
#endif

    bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found);
    if (found) {
        u_sess->instr_cxt.pg_buffer_usage->local_blks_hit++;
    } else {
        u_sess->instr_cxt.pg_buffer_usage->local_blks_read++;
        pgstatCountLocalBlocksRead4SessionLevel();
    }

    /* At this point we do NOT hold any locks.
     *
     * if it was already in the buffer pool, we're done
     */
    if (found) {
        if (!isExtend) {
            /* Just need to update stats before we exit */
            *hit = true;
            t_thrd.vacuum_cxt.VacuumPageHit++;

            if (t_thrd.vacuum_cxt.VacuumCostActive)
                t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageHit;
            return BufferDescriptorGetBuffer(bufHdr);
        }

        /*
         * We get here only in the corner case where we are trying to extend
         * the relation but we found a pre-existing buffer marked BM_VALID.
         * This can happen because mdread doesn't complain about reads beyond
         * EOF (when u_sess->attr.attr_security.zero_damaged_pages is ON) and so a previous attempt to
         * read a block beyond EOF could have left a "valid" zero-filled
         * buffer.	Unfortunately, we have also seen this case occurring
         * because of buggy Linux kernels that sometimes return an
         * lseek(SEEK_END) result that doesn't account for a recent write. In
         * that situation, the pre-existing buffer would contain valid data
         * that we don't want to overwrite.  Since the legitimate case should
         * always have left a zero-filled buffer, complain if not PageIsNew.
         */
        bufBlock = LocalBufHdrGetBlock(bufHdr);
        if (!PageIsNew((Page)bufBlock)) {
            ereport(PANIC,
                    (errmsg("unexpected data beyond EOF in block %u of relation %s", blockNum,
                            relpath(smgr->smgr_rnode, forkNum)),
                     errdetail(
                         "buffer id: %d, page info: lsn %X/%X checksum 0x%X flags %hu lower %hu upper %hu special %hu",
                         bufHdr->buf_id, ((PageHeader)bufBlock)->pd_lsn.xlogid, ((PageHeader)bufBlock)->pd_lsn.xrecoff,
                         (uint)(((PageHeader)bufBlock)->pd_checksum), ((PageHeader)bufBlock)->pd_flags,
                         ((PageHeader)bufBlock)->pd_lower, ((PageHeader)bufBlock)->pd_upper,
                         ((PageHeader)bufBlock)->pd_special),
                     errhint("This has been seen to occur with buggy kernels; consider updating your system.")));
        }

        /*
         * We *must* do smgrextend before succeeding, else the page will not
         * be reserved by the kernel, and the next P_NEW call will decide to
         * return the same page.  Clear the BM_VALID bit, do the StartBufferIO
         * call that BufferAlloc didn't, and proceed.
         */

        /* Only need to adjust flags */
        uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);

        Assert(buf_state & BM_VALID);
        buf_state &= ~BM_VALID;
        pg_atomic_write_u32(&bufHdr->state, buf_state);
    }

    /*
     * if we have gotten to this point, we have allocated a buffer for the
     * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
     * if it's a shared buffer.
     *
     * Note: if smgrextend fails, we will end up with a buffer that is
     * allocated but not marked BM_VALID.  P_NEW will still select the same
     * block number (because the relation didn't get any longer on disk) and
     * so future attempts to extend the relation will find the same buffer (if
     * it's not been recycled) but come right back here to try smgrextend
     * again.
     */
    Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID)); /* spinlock not needed */

    bufBlock = LocalBufHdrGetBlock(bufHdr);

    (void)ReadBuffer_common_ReadBlock(smgr, relpersistence, forkNum, blockNum, mode,
                                      isExtend, bufBlock, NULL, &need_reapir);

    uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);
    buf_state |= BM_VALID;
    pg_atomic_write_u32(&bufHdr->state, buf_state);

    return BufferDescriptorGetBuffer(bufHdr);
}

/*
 * ReadBuffer_common_for_direct -- fast read block
 *
 * *batch redo add 2020-03-04
 */
Buffer ReadBuffer_common_for_direct(RelFileNode rnode, char relpersistence, ForkNumber forkNum, BlockNumber blockNum,
                                    ReadBufferMode mode)
{
    Block bufBlock;
    bool isExtend = false;
    RedoMemSlot *bufferslot = nullptr;
    bool need_reapir = false;

    isExtend = (blockNum == P_NEW);
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    /* Substitute proper block number if caller asked for P_NEW */
    if (isExtend)
        blockNum = smgrnblocks(smgr, forkNum);

    XLogRedoBufferAllocFunc(smgr->smgr_rnode.node, forkNum, blockNum, &bufferslot);

    XLogRedoBufferGetBlkFunc(bufferslot, &bufBlock);

    Assert(bufBlock != NULL);
    (void)ReadBuffer_common_ReadBlock(smgr, relpersistence, forkNum, blockNum, mode,
                                      isExtend, bufBlock, NULL, &need_reapir);
    if (need_reapir) {
        return InvalidBuffer;
    }
    XLogRedoBufferSetStateFunc(bufferslot, BM_VALID);
    return RedoBufferSlotGetBuffer(bufferslot);
}

/*
 * ReadBuffer_common_ReadBlock -- common logic for all ReadBuffer variants
 *  reconstruct for batch redo
 * * 2020-03-05
 */
static bool ReadBuffer_common_ReadBlock(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
    BlockNumber blockNum, ReadBufferMode mode, bool isExtend, Block bufBlock, const XLogPhyBlock *pblk,
    bool *need_repair)
{
    bool needputtodirty = false;

    if (isExtend) {
        /* new buffers are zero-filled */
        MemSet((char *)bufBlock, 0, BLCKSZ);
        smgrextend(smgr, forkNum, blockNum, (char *)bufBlock, false);


        /*
         * NB: we're *not* doing a ScheduleBufferTagForWriteback here;
         * although we're essentially performing a write. At least on linux
         * doing so defeats the 'delayed allocation' mechanism, leading to
         * increased file fragmentation.
         */
    } else {
        /*
         * Read in the page, unless the caller intends to overwrite it and
         * just wants us to allocate a buffer.
         */
        if (mode == RBM_ZERO || mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) {
            MemSet((char *)bufBlock, 0, BLCKSZ);
        } else {
            instr_time io_start, io_time;

            INSTR_TIME_SET_CURRENT(io_start);

            SMGR_READ_STATUS rdStatus;
            if (pblk != NULL) {
                SegmentCheck(XLOG_NEED_PHYSICAL_LOCATION(smgr->smgr_rnode.node));
                SegmentCheck(PhyBlockIsValid(*pblk));
                SegSpace* spc = spc_open(smgr->smgr_rnode.node.spcNode, smgr->smgr_rnode.node.dbNode, false);
                SegmentCheck(spc);
                RelFileNode fakenode = {
                    .spcNode = spc->spcNode,
                    .dbNode = spc->dbNode,
                    .relNode = pblk->relNode,
                    .bucketNode = SegmentBktId,
                    .opt = 0
                };
                seg_physical_read(spc, fakenode, forkNum, pblk->block, (char *)bufBlock);
                if (PageIsVerified((Page)bufBlock, pblk->block)) {
                    rdStatus =  SMGR_RD_OK;
                } else {
                    rdStatus =  SMGR_RD_CRC_ERROR;
                }
            } else {
                rdStatus = smgrread(smgr, forkNum, blockNum, (char *)bufBlock);
            }

            if (u_sess->attr.attr_common.track_io_timing) {
                INSTR_TIME_SET_CURRENT(io_time);
                INSTR_TIME_SUBTRACT(io_time, io_start);
                pgstat_count_buffer_read_time(INSTR_TIME_GET_MICROSEC(io_time));
                INSTR_TIME_ADD(u_sess->instr_cxt.pg_buffer_usage->blk_read_time, io_time);
                pgstatCountBlocksReadTime4SessionLevel(INSTR_TIME_GET_MICROSEC(io_time));
            } else {
                INSTR_TIME_SET_CURRENT(io_time);
                INSTR_TIME_SUBTRACT(io_time, io_start);
                pgstatCountBlocksReadTime4SessionLevel(INSTR_TIME_GET_MICROSEC(io_time));
            }

            /* check for garbage data */
            if (rdStatus == SMGR_RD_CRC_ERROR) {
                addBadBlockStat(&smgr->smgr_rnode.node, forkNum);
                if (!RecoveryInProgress()) {
                    addGlobalRepairBadBlockStat(smgr->smgr_rnode, forkNum, blockNum);
                }

                if (mode == RBM_ZERO_ON_ERROR || u_sess->attr.attr_security.zero_damaged_pages) {
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                      errmsg("invalid page in block %u of relation %s; zeroing out page", blockNum,
                                             relpath(smgr->smgr_rnode, forkNum)),
                                      handle_in_client(true)));
                    MemSet((char *)bufBlock, 0, BLCKSZ);
                } else if (mode != RBM_FOR_REMOTE && relpersistence == RELPERSISTENCE_PERMANENT &&
                           CanRemoteRead() && !IsSegmentFileNode(smgr->smgr_rnode.node)) {
                    /* not alread in remote read and not temp/unlogged table, try to remote read */
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                      errmsg("invalid page in block %u of relation %s, try to remote read", blockNum,
                                             relpath(smgr->smgr_rnode, forkNum)),
                                      handle_in_client(true)));

                    RemoteReadBlock(smgr->smgr_rnode, forkNum, blockNum, (char *)bufBlock, NULL);

                    if (PageIsVerified((Page)bufBlock, blockNum)) {
                        needputtodirty = true;
                        UpdateRepairTime(smgr->smgr_rnode.node, forkNum, blockNum);
                    } else
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                        errmsg("invalid page in block %u of relation %s, remote read data corrupted",
                                               blockNum, relpath(smgr->smgr_rnode, forkNum))));
                } else {
                    /* record bad page, wait the pagerepair thread repair the page */
                    *need_repair = CheckVerionSupportRepair() &&
                        (AmStartupProcess() || AmPageRedoWorker()) && IsPrimaryClusterStandbyDN() &&
                        g_instance.repair_cxt.support_repair;
                    if (*need_repair) {
                        RepairBlockKey key;
                        XLogPhyBlock pblk_bak = {0};
                        key.relfilenode = smgr->smgr_rnode.node;
                        key.forknum = forkNum;
                        key.blocknum = blockNum;
                        if (pblk != NULL) {
                            pblk_bak = *pblk;
                        }
                        RedoPageRepairCallBack(key, pblk_bak);
                        log_invalid_page(smgr->smgr_rnode.node, forkNum, blockNum, CRC_CHECK_ERROR, pblk);
                        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("invalid page in block %u of relation %s",
                                blockNum, relpath(smgr->smgr_rnode, forkNum))));
                        return false;
                    }
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("invalid page in block %u of relation %s",
                            blockNum, relpath(smgr->smgr_rnode, forkNum))));
                }
            }

            PageDataDecryptIfNeed((Page)bufBlock);
        }
    }

    return needputtodirty;
}

static inline void BufferDescSetPBLK(BufferDesc *buf, const XLogPhyBlock *pblk)
{
    if (pblk != NULL) {
        buf->seg_fileno = pblk->relNode;
        buf->seg_blockno = pblk->block;
    }
}

/*
 * ReadBuffer_common -- common logic for all ReadBuffer variants
 *
 * *hit is set to true if the request was satisfied from shared buffer cache.
 */
static Buffer ReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum, BlockNumber blockNum,
                                ReadBufferMode mode, BufferAccessStrategy strategy, bool *hit, const XLogPhyBlock *pblk)
{
    BufferDesc *bufHdr = NULL;
    Block bufBlock;
    bool found = false;
    bool isExtend = false;
    bool isLocalBuf = SmgrIsTemp(smgr);
    bool need_repair = false;

    *hit = false;

    /* Make sure we will have room to remember the buffer pin */
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    isExtend = (blockNum == P_NEW);

    TRACE_POSTGRESQL_BUFFER_READ_START(forkNum, blockNum, smgr->smgr_rnode.node.spcNode, smgr->smgr_rnode.node.dbNode,
                                       smgr->smgr_rnode.node.relNode, smgr->smgr_rnode.backend, isExtend);

    /* Substitute proper block number if caller asked for P_NEW */
    if (isExtend) {
        blockNum = smgrnblocks(smgr, forkNum);
    /* When the parallel redo is enabled, there may be a scenario where
     * the index is replayed before the page replayed. For single-mode,
     * readable standby feature, Operators related to index scan access
     * the index first, then access the table, and you will find that
     * the tid or the heap(page) does not exist. Because the transaction
     * was not originally committed, the tid or the heap(page) should not
     * be visible. So accessing the non-existent heap tuple by the tid
     * should return that the tuple does not exist without error reporting.
     *
     * Segment-page storage does not support standby reading. Its segment
     * head may be re-used, i.e., the relfilenode may be reused. Thus the
     * smgrnblocks interface can not be used on standby. Just skip this check.
     */
    } else if (RecoveryInProgress() && !IsSegmentFileNode(smgr->smgr_rnode.node)) {
        BlockNumber totalBlkNum = smgrnblocks_cached(smgr, forkNum);

        /* Update cached blocks */
        if (totalBlkNum == InvalidBlockNumber || blockNum >= totalBlkNum) {
            totalBlkNum = smgrnblocks(smgr, forkNum);
        }

        if (blockNum >= totalBlkNum) {
            return InvalidBuffer;
        }
    }
    if (isLocalBuf) {
        bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found);
        if (found) {
            u_sess->instr_cxt.pg_buffer_usage->local_blks_hit++;
        } else {
            u_sess->instr_cxt.pg_buffer_usage->local_blks_read++;
            pgstatCountLocalBlocksRead4SessionLevel();
        }
    } else {
        /*
         * lookup the buffer.  IO_IN_PROGRESS is set if the requested block is
         * not currently in memory.
         */
        bufHdr = BufferAlloc(smgr, relpersistence, forkNum, blockNum, strategy, &found, pblk);
        if (g_instance.attr.attr_security.enable_tde && IS_PGXC_DATANODE) {
            bufHdr->encrypt = smgr->encrypt ? true : false; /* set tde flag */
        }
        if (found) {
            u_sess->instr_cxt.pg_buffer_usage->shared_blks_hit++;
        } else {
            u_sess->instr_cxt.pg_buffer_usage->shared_blks_read++;
            pgstatCountSharedBlocksRead4SessionLevel();
        }
    }

    /* At this point we do NOT hold any locks.
     *
     * if it was already in the buffer pool, we're done
     */
    if (found) {
        if (!isExtend) {
            /* Just need to update stats before we exit */
            *hit = true;
            t_thrd.vacuum_cxt.VacuumPageHit++;

            if (t_thrd.vacuum_cxt.VacuumCostActive)
                t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageHit;

            TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum, smgr->smgr_rnode.node.spcNode,
                                              smgr->smgr_rnode.node.dbNode, smgr->smgr_rnode.node.relNode,
                                              smgr->smgr_rnode.backend, isExtend, found);

            /*
             * In RBM_ZERO_AND_LOCK mode the caller expects the page to
             * be locked on return.
             */
            if (!isLocalBuf) {
                if (mode == RBM_ZERO_AND_LOCK) {
                    LWLockAcquire(bufHdr->content_lock, LW_EXCLUSIVE);
                    /*
                     * A corner case in segment-page storage:
                     * a block is moved by segment space shrink, and its physical location is changed. But physical
                     * location cached in BufferDesc is old. We should update its physical location during the
                     * movement xlog, which must read buffer with RBM_ZERO_AND_LOCK mode. So we update pblk here.
                     *
                     * Exclusive protects us with concurrent buffer flush.
                     */
                    BufferDescSetPBLK(bufHdr, pblk);
                } else if (mode == RBM_ZERO_AND_CLEANUP_LOCK) {
                    LockBufferForCleanup(BufferDescriptorGetBuffer(bufHdr));
                }
            }

            return BufferDescriptorGetBuffer(bufHdr);
        }

        /*
         * We get here only in the corner case where we are trying to extend
         * the relation but we found a pre-existing buffer marked BM_VALID.
         * This can happen because mdread doesn't complain about reads beyond
         * EOF (when u_sess->attr.attr_security.zero_damaged_pages is ON) and so a previous attempt to
         * read a block beyond EOF could have left a "valid" zero-filled
         * buffer.	Unfortunately, we have also seen this case occurring
         * because of buggy Linux kernels that sometimes return an
         * lseek(SEEK_END) result that doesn't account for a recent write. In
         * that situation, the pre-existing buffer would contain valid data
         * that we don't want to overwrite.  Since the legitimate case should
         * always have left a zero-filled buffer, complain if not PageIsNew.
         */
        bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);
        if (!PageIsNew((Page)bufBlock)) {
            ereport(PANIC,
                    (errmsg("unexpected data beyond EOF in block %u of relation %s", blockNum,
                            relpath(smgr->smgr_rnode, forkNum)),
                     errdetail(
                         "buffer id: %d, page info: lsn %X/%X checksum 0x%X flags %hu lower %hu upper %hu special %hu",
                         bufHdr->buf_id, ((PageHeader)bufBlock)->pd_lsn.xlogid, ((PageHeader)bufBlock)->pd_lsn.xrecoff,
                         (uint)(((PageHeader)bufBlock)->pd_checksum), ((PageHeader)bufBlock)->pd_flags,
                         ((PageHeader)bufBlock)->pd_lower, ((PageHeader)bufBlock)->pd_upper,
                         ((PageHeader)bufBlock)->pd_special),
                     errhint("This has been seen to occur with buggy kernels; consider updating your system.")));
        }

        /*
         * We *must* do smgrextend before succeeding, else the page will not
         * be reserved by the kernel, and the next P_NEW call will decide to
         * return the same page.  Clear the BM_VALID bit, do the StartBufferIO
         * call that BufferAlloc didn't, and proceed.
         */
        if (isLocalBuf) {
            /* Only need to adjust flags */
            uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);

            Assert(buf_state & BM_VALID);
            buf_state &= ~BM_VALID;
            pg_atomic_write_u32(&bufHdr->state, buf_state);
        } else {
            /*
             * Loop to handle the very small possibility that someone re-sets
             * BM_VALID between our clearing it and StartBufferIO inspecting
             * it.
             */
            do {
                uint32 buf_state = LockBufHdr(bufHdr);

                Assert(buf_state & BM_VALID);
                buf_state &= ~BM_VALID;
                UnlockBufHdr(bufHdr, buf_state);
            } while (!StartBufferIO(bufHdr, true));
        }
    }

    /*
     * if we have gotten to this point, we have allocated a buffer for the
     * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
     * if it's a shared buffer.
     *
     * Note: if smgrextend fails, we will end up with a buffer that is
     * allocated but not marked BM_VALID.  P_NEW will still select the same
     * block number (because the relation didn't get any longer on disk) and
     * so future attempts to extend the relation will find the same buffer (if
     * it's not been recycled) but come right back here to try smgrextend
     * again.
     */
    Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID)); /* spinlock not needed */

    bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);

    bool needputtodirty = ReadBuffer_common_ReadBlock(smgr, relpersistence, forkNum, blockNum,
                                                      mode, isExtend, bufBlock, pblk, &need_repair);
    if (need_repair) {
        LWLockRelease(((BufferDesc *)bufHdr)->io_in_progress_lock);
        UnpinBuffer(bufHdr, true);
        AbortBufferIO();
        return InvalidBuffer;
    }
    if (needputtodirty) {
        /* set  BM_DIRTY to overwrite later */
        uint32 old_buf_state = LockBufHdr(bufHdr);
        uint32 buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);

        /*
         * When the page is marked dirty for the first time, needs to push the dirty page queue.
         * Check the BufferDesc rec_lsn to determine whether the dirty page is in the dirty page queue.
         * If the rec_lsn is valid, dirty page is already in the queue, don't need to push it again.
         */
        if (ENABLE_INCRE_CKPT) {
            for (;;) {
                buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);
                if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&bufHdr->rec_lsn))) {
                    break;
                }

                if (!is_dirty_page_queue_full(bufHdr) && push_pending_flush_queue(BufferDescriptorGetBuffer(bufHdr))) {
                    break;
                }
                UnlockBufHdr(bufHdr, old_buf_state);
                pg_usleep(TEN_MICROSECOND);
                old_buf_state = LockBufHdr(bufHdr);
            }
        }
        UnlockBufHdr(bufHdr, buf_state);
    }

    /*
     * In RBM_ZERO_AND_LOCK mode, grab the buffer content lock before marking
     * the page as valid, to make sure that no other backend sees the zeroed
     * page before the caller has had a chance to initialize it.
     *
     * Since no-one else can be looking at the page contents yet, there is no
     * difference between an exclusive lock and a cleanup-strength lock.
     * (Note that we cannot use LockBuffer() of LockBufferForCleanup() here,
     * because they assert that the buffer is already valid.)
     */
    if ((mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) && !isLocalBuf) {
        LWLockAcquire(bufHdr->content_lock, LW_EXCLUSIVE);
    }

    if (isLocalBuf) {
        /* Only need to adjust flags */
        uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);

        buf_state |= BM_VALID;
        pg_atomic_write_u32(&bufHdr->state, buf_state);
    } else {
        bufHdr->lsn_on_disk = PageGetLSN(bufBlock);
#ifdef USE_ASSERT_CHECKING
        bufHdr->lsn_dirty = InvalidXLogRecPtr;
#endif
        /* Set BM_VALID, terminate IO, and wake up any waiters */
        TerminateBufferIO(bufHdr, false, BM_VALID);
    }

    t_thrd.vacuum_cxt.VacuumPageMiss++;
    if (t_thrd.vacuum_cxt.VacuumCostActive)
        t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageMiss;

    TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum, smgr->smgr_rnode.node.spcNode, smgr->smgr_rnode.node.dbNode,
                                      smgr->smgr_rnode.node.relNode, smgr->smgr_rnode.backend, isExtend, found);

    return BufferDescriptorGetBuffer(bufHdr);
}

void SimpleMarkBufDirty(BufferDesc *buf)
{
    /* set  BM_DIRTY to overwrite later */
    uint32 oldBufState = LockBufHdr(buf);
    uint32 bufState = oldBufState | (BM_DIRTY | BM_JUST_DIRTIED);

    /*
     * When the page is marked dirty for the first time, needs to push the dirty page queue.
     * Check the BufferDesc rec_lsn to determine whether the dirty page is in the dirty page queue.
     * If the rec_lsn is valid, dirty page is already in the queue, don't need to push it again.
     */
    if (ENABLE_INCRE_CKPT) {
        for (;;) {
            bufState = oldBufState | (BM_DIRTY | BM_JUST_DIRTIED);
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->rec_lsn))) {
                break;
            }

            if (!is_dirty_page_queue_full(buf) && push_pending_flush_queue(BufferDescriptorGetBuffer(buf))) {
                break;
            }
            UnlockBufHdr(buf, oldBufState);
            pg_usleep(TEN_MICROSECOND);
            oldBufState = LockBufHdr(buf);
        }
    }
    UnlockBufHdr(buf, bufState);

}

static void PageCheckIfCanEliminate(BufferDesc *buf, uint32 *oldFlags, bool *needGetLock)
{
    Block tmpBlock = BufHdrGetBlock(buf);

    if ((*oldFlags & BM_TAG_VALID) && !XLByteEQ(buf->lsn_on_disk, PageGetLSN(tmpBlock)) && !(*oldFlags & BM_DIRTY) &&
        RecoveryInProgress()) {
        int mode = DEBUG1;
#ifdef USE_ASSERT_CHECKING
        mode = PANIC;
#endif
        const uint32 shiftSize = 32;
        ereport(mode, (errmodule(MOD_INCRE_BG),
                errmsg("check lsn is not matched on disk:%X/%X on page %X/%X, relnode info:%u/%u/%u %u %u",
                       (uint32)(buf->lsn_on_disk >> shiftSize), (uint32)(buf->lsn_on_disk),
                       (uint32)(PageGetLSN(tmpBlock) >> shiftSize), (uint32)(PageGetLSN(tmpBlock)),
                       buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode,
                       buf->tag.blockNum, buf->tag.forkNum)));
        SimpleMarkBufDirty(buf);
        *oldFlags |= BM_DIRTY;
        *needGetLock = true;
    }
}

#ifdef USE_ASSERT_CHECKING
static void PageCheckWhenChosedElimination(const BufferDesc *buf, uint32 oldFlags)
{
    if ((oldFlags & BM_TAG_VALID) && RecoveryInProgress()) {
        if (!XLByteEQ(buf->lsn_dirty, InvalidXLogRecPtr)) {
            Assert(XLByteEQ(buf->lsn_on_disk, buf->lsn_dirty));
        }
    }
}
#endif


/*
 * BufferAlloc -- subroutine for ReadBuffer.  Handles lookup of a shared
 *		buffer.  If no buffer exists already, selects a replacement
 *		victim and evicts the old page, but does NOT read in new page.
 *
 * "strategy" can be a buffer replacement strategy object, or NULL for
 * the default strategy.  The selected buffer's usage_count is advanced when
 * using the default strategy, but otherwise possibly not (see PinBuffer).
 *
 * The returned buffer is pinned and is already marked as holding the
 * desired page.  If it already did have the desired page, *foundPtr is
 * set TRUE.  Otherwise, *foundPtr is set FALSE and the buffer is marked
 * as IO_IN_PROGRESS; ReadBuffer will now need to do I/O to fill it.
 *
 * *foundPtr is actually redundant with the buffer's BM_VALID flag, but
 * we keep it for simplicity in ReadBuffer.
 *
 * No locks are held either at entry or exit.
 */
static BufferDesc *BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber fork_num, BlockNumber block_num,
                               BufferAccessStrategy strategy, bool *found, const XLogPhyBlock *pblk)
{
    Assert(!IsSegmentPhysicalRelNode(smgr->smgr_rnode.node));

    BufferTag new_tag;                 /* identity of requested block */
    uint32 new_hash;                   /* hash value for newTag */
    LWLock *new_partition_lock = NULL; /* buffer partition lock for it */
    BufferTag old_tag;                 /* previous identity of selected buffer */
    uint32 old_hash;                   /* hash value for oldTag */
    LWLock *old_partition_lock = NULL; /* buffer partition lock for it */
    uint32 old_flags;
    int buf_id;
    BufferDesc *buf = NULL;
    bool valid = false;
    uint32 buf_state;

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, fork_num, block_num);

    /* determine its hash code and partition lock ID */
    new_hash = BufTableHashCode(&new_tag);
    new_partition_lock = BufMappingPartitionLock(new_hash);

    /* see if the block is in the buffer pool already */
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    pgstat_report_waitevent(WAIT_EVENT_BUF_HASH_SEARCH);
    buf_id = BufTableLookup(&new_tag, new_hash);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (buf_id >= 0) {
        /*
         * Found it.  Now, pin the buffer so no one can steal it from the
         * buffer pool, and check to see if the correct data has been loaded
         * into the buffer.
         */
        buf = GetBufferDescriptor(buf_id);

        valid = PinBuffer(buf, strategy);

        /* Can release the mapping lock as soon as we've pinned it */
        LWLockRelease(new_partition_lock);

        *found = TRUE;

        if (!valid) {
            /*
             * We can only get here if (a) someone else is still reading in
             * the page, or (b) a previous read attempt failed.  We have to
             * wait for any active read attempt to finish, and then set up our
             * own read attempt if the page is still not BM_VALID.
             * StartBufferIO does it all.
             */
            if (StartBufferIO(buf, true)) {
                /*
                 * If we get here, previous attempts to read the buffer must
                 * have failed ... but we shall bravely try again.
                 */
                *found = FALSE;
            }
        }

        return buf;
    }

    /*
     * Didn't find it in the buffer pool.  We'll have to initialize a new
     * buffer.	Remember to unlock the mapping lock while doing the work.
     */
    LWLockRelease(new_partition_lock);
    /* Loop here in case we have to try another victim buffer */
    for (;;) {
        bool needGetLock = false;
        /*
         * Select a victim buffer.	The buffer is returned with its header
         * spinlock still held!
         */
        pgstat_report_waitevent(WAIT_EVENT_BUF_STRATEGY_GET);
        buf = (BufferDesc *)StrategyGetBuffer(strategy, &buf_state);
        pgstat_report_waitevent(WAIT_EVENT_END);

        Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

        /* Must copy buffer flags while we still hold the spinlock */
        old_flags = buf_state & BUF_FLAG_MASK;

        /* Pin the buffer and then release the buffer spinlock */
        PinBuffer_Locked(buf);

        PageCheckIfCanEliminate(buf, &old_flags, &needGetLock);
        /*
         * If the buffer was dirty, try to write it out.  There is a race
         * condition here, in that someone might dirty it after we released it
         * above, or even while we are writing it out (since our share-lock
         * won't prevent hint-bit updates).  We will recheck the dirty bit
         * after re-locking the buffer header.
         */
        if (old_flags & BM_DIRTY) {
            /* backend should not flush dirty pages if working version less than DW_SUPPORT_NEW_SINGLE_FLUSH */
            if (!backend_can_flush_dirty_page()) {
                UnpinBuffer(buf, true);
                (void)sched_yield();
                continue;
            }

            /*
             * We need a share-lock on the buffer contents to write it out
             * (else we might write invalid data, eg because someone else is
             * compacting the page contents while we write).  We must use a
             * conditional lock acquisition here to avoid deadlock.  Even
             * though the buffer was not pinned (and therefore surely not
             * locked) when StrategyGetBuffer returned it, someone else could
             * have pinned and exclusive-locked it by the time we get here. If
             * we try to get the lock unconditionally, we'd block waiting for
             * them; if they later block waiting for us, deadlock ensues.
             * (This has been observed to happen when two backends are both
             * trying to split btree index pages, and the second one just
             * happens to be trying to split the page the first one got from
             * StrategyGetBuffer.)
             */
            bool needDoFlush = false;
            if (!needGetLock) {
                needDoFlush = LWLockConditionalAcquire(buf->content_lock, LW_SHARED);
            } else {
                LWLockAcquire(buf->content_lock, LW_SHARED);
                needDoFlush = true;
            }
            if (needDoFlush) {
                /*
                 * If using a nondefault strategy, and writing the buffer
                 * would require a WAL flush, let the strategy decide whether
                 * to go ahead and write/reuse the buffer or to choose another
                 * victim.	We need lock to inspect the page LSN, so this
                 * can't be done inside StrategyGetBuffer.
                 */
                if (strategy != NULL) {
                    XLogRecPtr lsn;

                    /* Read the LSN while holding buffer header lock */
                    buf_state = LockBufHdr(buf);
                    lsn = BufferGetLSN(buf);
                    UnlockBufHdr(buf, buf_state);

                    if (XLogNeedsFlush(lsn) && StrategyRejectBuffer(strategy, buf)) {
                        /* Drop lock/pin and loop around for another buffer */
                        LWLockRelease(buf->content_lock);
                        UnpinBuffer(buf, true);
                        continue;
                    }
                }

                /* OK, do the I/O */
                TRACE_POSTGRESQL_BUFFER_WRITE_DIRTY_START(fork_num, block_num, smgr->smgr_rnode.node.spcNode,
                                                          smgr->smgr_rnode.node.dbNode, smgr->smgr_rnode.node.relNode);

                /* during initdb, not need flush dw file */
                if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
                    if (!free_space_enough(buf->buf_id)) {
                        LWLockRelease(buf->content_lock);
                        UnpinBuffer(buf, true);
                        continue;
                    }
                    uint32 pos = 0;
                    pos = first_version_dw_single_flush(buf);
                    t_thrd.proc->dw_pos = pos;
                    FlushBuffer(buf, NULL);
                    g_instance.dw_single_cxt.single_flush_state[pos] = true;
                    t_thrd.proc->dw_pos = -1;
                } else {
                    FlushBuffer(buf, NULL);
                }
                LWLockRelease(buf->content_lock);

                ScheduleBufferTagForWriteback(t_thrd.storage_cxt.BackendWritebackContext, &buf->tag);

                TRACE_POSTGRESQL_BUFFER_WRITE_DIRTY_DONE(fork_num, block_num, smgr->smgr_rnode.node.spcNode,
                                                         smgr->smgr_rnode.node.dbNode, smgr->smgr_rnode.node.relNode);
            } else {
                /*
                 * Someone else has locked the buffer, so give it up and loop
                 * back to get another one.
                 */
                UnpinBuffer(buf, true);
                continue;
            }
        }

        /*
         * To change the association of a valid buffer, we'll need to have
         * exclusive lock on both the old and new mapping partitions.
         */
        if (old_flags & BM_TAG_VALID) {
            /*
             * Need to compute the old tag's hashcode and partition lock ID.
             * XXX is it worth storing the hashcode in BufferDesc so we need
             * not recompute it here?  Probably not.
             */
            old_tag = ((BufferDesc *)buf)->tag;
            old_hash = BufTableHashCode(&old_tag);
            old_partition_lock = BufMappingPartitionLock(old_hash);
            /*
             * Must lock the lower-numbered partition first to avoid
             * deadlocks.
             */
            if (old_partition_lock < new_partition_lock) {
                (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            } else if (old_partition_lock > new_partition_lock) {
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
                (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
            } else {
                /* only one partition, only one lock */
                (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            }
        } else {
            /* if it wasn't valid, we need only the new partition */
            (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            /* these just keep the compiler quiet about uninit variables */
            old_hash = 0;
            old_partition_lock = NULL;
        }

        /*
         * Try to make a hashtable entry for the buffer under its new tag.
         * This could fail because while we were writing someone else
         * allocated another buffer for the same block we want to read in.
         * Note that we have not yet removed the hashtable entry for the old
         * tag.
         */
        buf_id = BufTableInsert(&new_tag, new_hash, buf->buf_id);
        if (buf_id >= 0) {
            /*
             * Got a collision. Someone has already done what we were about to
             * do. We'll just handle this as if it were found in the buffer
             * pool in the first place.  First, give up the buffer we were
             * planning to use.
             */
            UnpinBuffer(buf, true);

            /* Can give up that buffer's mapping partition lock now */
            if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock)
                LWLockRelease(old_partition_lock);

            /* remaining code should match code at top of routine */
            buf = GetBufferDescriptor(buf_id);

            valid = PinBuffer(buf, strategy);

            /* Can release the mapping lock as soon as we've pinned it */
            LWLockRelease(new_partition_lock);

            *found = TRUE;

            if (!valid) {
                /*
                 * We can only get here if (a) someone else is still reading
                 * in the page, or (b) a previous read attempt failed.	We
                 * have to wait for any active read attempt to finish, and
                 * then set up our own read attempt if the page is still not
                 * BM_VALID.  StartBufferIO does it all.
                 */
                if (StartBufferIO(buf, true)) {
                    /*
                     * If we get here, previous attempts to read the buffer
                     * must have failed ... but we shall bravely try again.
                     */
                    *found = FALSE;
                }
            }

            return buf;
        }

        /*
         * Need to lock the buffer header too in order to change its tag.
         */
        buf_state = LockBufHdr(buf);

        /*
         * Somebody could have pinned or re-dirtied the buffer while we were
         * doing the I/O and making the new hashtable entry.  If so, we can't
         * recycle this buffer; we must undo everything we've done and start
         * over with a new victim buffer.
         */
        old_flags = buf_state & BUF_FLAG_MASK;
        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY) 
            && !(old_flags & BM_IS_META)) {
            break;
        }

        UnlockBufHdr(buf, buf_state);
        BufTableDelete(&new_tag, new_hash);
        if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
        LWLockRelease(new_partition_lock);
        UnpinBuffer(buf, true);
    }
    
#ifdef USE_ASSERT_CHECKING
    PageCheckWhenChosedElimination(buf, old_flags);
#endif

    /*
     * Okay, it's finally safe to rename the buffer.
     *
     * Clearing BM_VALID here is necessary, clearing the dirtybits is just
     * paranoia.  We also reset the usage_count since any recency of use of
     * the old content is no longer relevant.  (The usage_count starts out at
     * 1 so that the buffer can survive one clock-sweep pass.)
     *
     * Make sure BM_PERMANENT is set for buffers that must be written at every
     * checkpoint.  Unlogged buffers only need to be written at shutdown
     * checkpoints, except for their "init" forks, which need to be treated
     * just like permanent relations.
     */
    ((BufferDesc *)buf)->tag = new_tag;
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT |
                   BUF_USAGECOUNT_MASK);
    if (relpersistence == RELPERSISTENCE_PERMANENT || fork_num == INIT_FORKNUM ||
        ((relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED)) {
        buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
    } else {
        buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
    }

    UnlockBufHdr(buf, buf_state);

    if (old_flags & BM_TAG_VALID) {
        BufTableDelete(&old_tag, old_hash);
        if (old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
    }

    /* set Physical segment file. */
    if (pblk != NULL) {
        Assert(PhyBlockIsValid(*pblk));
        buf->seg_fileno = pblk->relNode;
        buf->seg_blockno = pblk->block;
    } else {
        buf->seg_fileno = EXTENT_INVALID;
        buf->seg_blockno = InvalidBlockNumber;
    }
    LWLockRelease(new_partition_lock);

    /*
     * Buffer contents are currently invalid.  Try to get the io_in_progress
     * lock.  If StartBufferIO returns false, then someone else managed to
     * read it before we did, so there's nothing left for BufferAlloc() to do.
     */
    if (StartBufferIO(buf, true)) {
        *found = FALSE;
    } else {
        *found = TRUE;
    }

    return buf;
}

/*
 * InvalidateBuffer -- mark a shared buffer invalid and return it to the
 * freelist.
 *
 * The buffer header spinlock must be held at entry.  We drop it before
 * returning.  (This is sane because the caller must have locked the
 * buffer in order to be sure it should be dropped.)
 *
 * This is used only in contexts such as dropping a relation.  We assume
 * that no other backend could possibly be interested in using the page,
 * so the only reason the buffer might be pinned is if someone else is
 * trying to write it out.	We have to let them finish before we can
 * reclaim the buffer.
 *
 * The buffer could get reclaimed by someone else while we are waiting
 * to acquire the necessary locks; if so, don't mess it up.
 */
void InvalidateBuffer(BufferDesc *buf)
{    
    BufferTag old_tag;
    uint32 old_hash;                   /* hash value for oldTag */
    LWLock *old_partition_lock = NULL; /* buffer partition lock for it */
    uint32 old_flags;
    uint32 buf_state;

    /* Save the original buffer tag before dropping the spinlock */
    old_tag = ((BufferDesc *)buf)->tag;

    buf_state = pg_atomic_read_u32(&buf->state);
    Assert(buf_state & BM_LOCKED);
    UnlockBufHdr(buf, buf_state);

    /*
     * Need to compute the old tag's hashcode and partition lock ID. XXX is it
     * worth storing the hashcode in BufferDesc so we need not recompute it
     * here?  Probably not.
     */
    old_hash = BufTableHashCode(&old_tag);
    old_partition_lock = BufMappingPartitionLock(old_hash);

retry:

    /*
     * Acquire exclusive mapping lock in preparation for changing the buffer's
     * association.
     */
    (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);

    /* Re-lock the buffer header */
    buf_state = LockBufHdr(buf);

    /* If it's changed while we were waiting for lock, do nothing */
    if (!BUFFERTAGS_EQUAL(buf->tag, old_tag)) {
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(old_partition_lock);
        return;
    }

    /*
     * We assume the only reason for it to be pinned is that someone else is
     * flushing the page out.  Wait for them to finish.  (This could be an
     * infinite loop if the refcount is messed up... it would be nice to time
     * out after awhile, but there seems no way to be sure how many loops may
     * be needed.  Note that if the other guy has pinned the buffer but not
     * yet done StartBufferIO, WaitIO will fall through and we'll effectively
     * be busy-looping here.)
     */
    if (BUF_STATE_GET_REFCOUNT(buf_state) != 0) {
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(old_partition_lock);
        /* safety check: should definitely not be our *own* pin */
        if (GetPrivateRefCount(buf->buf_id + 1) > 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER),
                            (errmsg("buffer is pinned in InvalidateBuffer %d", buf->buf_id))));
        }
        WaitIO(buf);
        goto retry;
    }

    /* remove from dirty page list */
    if (ENABLE_INCRE_CKPT && (buf_state & BM_DIRTY)) {
        if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->rec_lsn))) {
            remove_dirty_page_from_queue(buf);
        } else {
            ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errcode(ERRCODE_INVALID_BUFFER),
                            (errmsg("buffer is dirty but not in dirty page queue in InvalidateBuffer"))));
        }
    }

    /*
     * Clear out the buffer's tag and flags.  We must do this to ensure that
     * linear scans of the buffer array don't think the buffer is valid.
     */
    old_flags = buf_state & BUF_FLAG_MASK;
    CLEAR_BUFFERTAG(buf->tag);
    buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
    UnlockBufHdr(buf, buf_state);

    /*
     * Remove the buffer from the lookup hashtable, if it was in there.
     */
    if (old_flags & BM_TAG_VALID) {
        BufTableDelete(&old_tag, old_hash);
    }

    /*
     * Done with mapping lock.
     */
    LWLockRelease(old_partition_lock);
}

#ifdef USE_ASSERT_CHECKING
static void recheck_page_content(const BufferDesc *buf_desc)
{
    Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_desc));
    if (unlikely(PageIsNew(page))) {
        if (((PageHeader)page)->pd_checksum == 0 && ((PageHeader)page)->pd_flags == 0 && PageGetLSN(page) == 0) {
            size_t* pagebytes = (size_t*)page;
            bool all_zeroes = true;
            for (int i = 0; i < (int)(BLCKSZ / sizeof(size_t)); i++) {
                if (pagebytes[i] != 0) {
                    all_zeroes = false;
                    break;
                }
            }
            if (!all_zeroes) {
                ereport(PANIC,(
                    errmsg("error page, page is new, but page not all zero, rel %u/%u/%u, bucketNode %d, block %d",
                    buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
                    buf_desc->tag.rnode.bucketNode, buf_desc->tag.blockNum)));
            }
        }
    }
}
#endif
/*
 * MarkBufferDirty
 *
 *		Marks buffer contents as dirty (actual write happens later).
 *
 * Buffer must be pinned and exclusive-locked.	(If caller does not hold
 * exclusive lock, then somebody could be in process of writing the buffer,
 * leading to risk of bad data written to disk.)
 */
void MarkBufferDirty(Buffer buffer)
{
    BufferDesc *buf_desc = NULL;
    uint32 buf_state;
    uint32 old_buf_state;

    if (!BufferIsValid(buffer)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("bad buffer ID: %d", buffer))));
    }

    if (BufferIsLocal(buffer)) {
        MarkLocalBufferDirty(buffer);
        return;
    }

    buf_desc = GetBufferDescriptor(buffer - 1);

    Assert(BufferIsPinned(buffer));
    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(buf_desc->content_lock));

    old_buf_state = LockBufHdr(buf_desc);

    buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);

    /*
     * When the page is marked dirty for the first time, needs to push the dirty page queue.
     * Check the BufferDesc rec_lsn to determine whether the dirty page is in the dirty page queue.
     * If the rec_lsn is valid, dirty page is already in the queue, don't need to push it again.
     */
    if (ENABLE_INCRE_CKPT) {
        for (;;) {
            buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf_desc->rec_lsn))) {
                break;
            }

            if (!is_dirty_page_queue_full(buf_desc) && push_pending_flush_queue(buffer)) {
                break;
            }

            UnlockBufHdr(buf_desc, old_buf_state);
            pg_usleep(TEN_MICROSECOND);
            old_buf_state = LockBufHdr(buf_desc);
        }
    }

    UnlockBufHdr(buf_desc, buf_state);

    /*
     * If the buffer was not dirty already, do vacuum accounting.
     */
    if (!(old_buf_state & BM_DIRTY)) {
        t_thrd.vacuum_cxt.VacuumPageDirty++;
        u_sess->instr_cxt.pg_buffer_usage->shared_blks_dirtied++;

        pgstatCountSharedBlocksDirtied4SessionLevel();

        if (t_thrd.vacuum_cxt.VacuumCostActive) {
            t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageDirty;
        }
    }
#ifdef USE_ASSERT_CHECKING
    recheck_page_content(buf_desc);
#endif
}

void MarkBufferMetaFlag(Buffer bufId, bool isSet)
{
    BufferDesc *buf = GetBufferDescriptor(bufId - 1);
    uint32 bufState;
    uint32 oldBufState;
    for (;;) {
        oldBufState = pg_atomic_read_u32(&buf->state);
        if (oldBufState & BM_LOCKED) {
            oldBufState = WaitBufHdrUnlocked(buf);
        }
        bufState = oldBufState;
        if (isSet) {
            bufState |= BM_IS_META;
            ereport(DEBUG1, (errmsg("mark buffer %d meta buffer stat %u.", bufId, bufState)));
        } else {
            bufState &= ~(BM_IS_META);
            ereport(DEBUG1, (errmsg("unmark buffer %d meta buffer stat %u.", bufId, bufState)));
        }
        if (pg_atomic_compare_exchange_u32(&buf->state, &oldBufState, bufState)) {
            break;
        }
    }
}

/*
 * ReleaseAndReadBuffer -- combine ReleaseBuffer() and ReadBuffer()
 *
 * Formerly, this saved one cycle of acquiring/releasing the BufMgrLock
 * compared to calling the two routines separately.  Now it's mainly just
 * a convenience function.	However, if the passed buffer is valid and
 * already contains the desired block, we just return it as-is; and that
 * does save considerable work compared to a full release and reacquire.
 *
 * Note: it is OK to pass buffer == InvalidBuffer, indicating that no old
 * buffer actually needs to be released.  This case is the same as ReadBuffer,
 * but can save some tests in the caller.
 */
Buffer ReleaseAndReadBuffer(Buffer buffer, Relation relation, BlockNumber block_num)
{
    ForkNumber fork_num = MAIN_FORKNUM;
    BufferDesc *buf_desc = NULL;

    if (BufferIsValid(buffer)) {
        if (BufferIsLocal(buffer)) {
            buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[-buffer - 1];
            if (buf_desc->tag.blockNum == block_num && RelFileNodeEquals(buf_desc->tag.rnode, relation->rd_node) &&
                buf_desc->tag.forkNum == fork_num)
                return buffer;
            ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, buffer);
            u_sess->storage_cxt.LocalRefCount[-buffer - 1]--;
        } else {
            buf_desc = GetBufferDescriptor(buffer - 1);
            /* we have pin, so it's ok to examine tag without spinlock */
            if (buf_desc->tag.blockNum == block_num && RelFileNodeEquals(buf_desc->tag.rnode, relation->rd_node) &&
                buf_desc->tag.forkNum == fork_num)
                return buffer;
            UnpinBuffer(buf_desc, true);
        }
    }

    return ReadBuffer(relation, block_num);
}

/*
 * PinBuffer -- make buffer unavailable for replacement.
 *
 * For the default access strategy, the buffer's usage_count is incremented
 * when we first pin it; for other strategies we just make sure the usage_count
 * isn't zero.  (The idea of the latter is that we don't want synchronized
 * heap scans to inflate the count, but we need it to not be zero to discourage
 * other backends from stealing buffers from our ring.	As long as we cycle
 * through the ring faster than the global clock-sweep cycles, buffers in
 * our ring won't be chosen as victims for replacement by other backends.)
 *
 * This should be applied only to shared buffers, never local ones.
 *
 * Since buffers are pinned/unpinned very frequently, pin buffers without
 * taking the buffer header lock; instead update the state variable in loop of
 * CAS operations. Hopefully it's just a single CAS.
 *
 * Note that ResourceOwnerEnlargeBuffers must have been done already.
 *
 * Returns TRUE if buffer is BM_VALID, else FALSE.	This provision allows
 * some callers to avoid an extra spinlock cycle.
 */
bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
{
    int b = buf->buf_id;
    bool result = false;
    PrivateRefCountEntry *ref = NULL;

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    PrivateRefCountEntry *free_entry = NULL;
    ref = GetPrivateRefCountEntryFast(b + 1, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(b + 1, true, true, free_entry);
    }
    Assert(ref != NULL);

    if (ref->refcount == 0) {
        uint32 buf_state;
        uint32 old_buf_state;

        old_buf_state = pg_atomic_read_u32(&buf->state);
        for (;;) {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = WaitBufHdrUnlocked(buf);
            }

            buf_state = old_buf_state;

            /* increase refcount */
            buf_state += BUF_REFCOUNT_ONE;

            /* increase usagecount unless already max */
            if (BUF_STATE_GET_USAGECOUNT(buf_state) != BM_MAX_USAGE_COUNT) {
                buf_state += BUF_USAGECOUNT_ONE;
            }

            if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state, buf_state)) {
                result = (buf_state & BM_VALID) != 0;
                break;
            }
        }
    } else {
        /* If we previously pinned the buffer, it must surely be valid */
        result = true;
    }
    ref->refcount++;
    Assert(ref->refcount > 0);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));
    return result;
}

/*
 * PinBuffer_Locked -- as above, but caller already locked the buffer header.
 * The spinlock is released before return.
 *
 * Currently, no callers of this function want to modify the buffer's
 * usage_count at all, so there's no need for a strategy parameter.
 * Also we don't bother with a BM_VALID test (the caller could check that for
 * itself).
 *
 * Note: use of this routine is frequently mandatory, not just an optimization
 * to save a spin lock/unlock cycle, because we need to pin a buffer before
 * its state can change under us.
 */
void PinBuffer_Locked(volatile BufferDesc *buf)
{
    int b = buf->buf_id;
    PrivateRefCountEntry *ref = NULL;
    uint32 buf_state;

    /* if error happend in GetPrivateRefCountEntry , can not do UnlockBufHdr */
    PrivateRefCountEntry *free_entry = NULL;
    ref = GetPrivateRefCountEntryFast(b + 1, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(b + 1, true, true, free_entry);
    }

    /*
     * Since we hold the buffer spinlock, we can update the buffer state and
     * release the lock in one operation.
     */
    buf_state = pg_atomic_read_u32(&buf->state);
    Assert(buf_state & BM_LOCKED);

    if (ref->refcount == 0) {
        buf_state += BUF_REFCOUNT_ONE;
    }
    UnlockBufHdr(buf, buf_state);
    ref->refcount++;
    Assert(ref->refcount > 0);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));
}

/*
 * UnpinBuffer -- make buffer available for replacement.
 *
 * This should be applied only to shared buffers, never local ones.
 *
 * Most but not all callers want CurrentResourceOwner to be adjusted.
 * Those that don't should pass fixOwner = FALSE.
 */
void UnpinBuffer(BufferDesc *buf, bool fixOwner)
{
    PrivateRefCountEntry *ref = NULL;
    int b = buf->buf_id;

    /* if error happend in GetPrivateRefCountEntry , can not do UnlockBufHdr */
    PrivateRefCountEntry *free_entry = NULL;
    ref = GetPrivateRefCountEntryFast(b + 1, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(b + 1, false, false, free_entry);
    }
    Assert(ref != NULL);

    if (fixOwner) {
        ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));
    }
    if (ref->refcount <= 0) {
        ereport(PANIC, (errmsg("[exception] private ref->refcount is %d in UnpinBuffer", ref->refcount)));
    }

    ref->refcount--;
    if (ref->refcount == 0) {
        uint32 buf_state;
        uint32 old_buf_state;

        /* I'd better not still hold any locks on the buffer */
        Assert(!LWLockHeldByMe(buf->content_lock));
        Assert(!LWLockHeldByMe(buf->io_in_progress_lock));

        /*
         * Decrement the shared reference count.
         *
         * Since buffer spinlock holder can update status using just write,
         * it's not safe to use atomic decrement here; thus use a CAS loop.
         */
        old_buf_state = pg_atomic_read_u32(&buf->state);
        for (;;) {
            if (old_buf_state & BM_LOCKED)
                old_buf_state = WaitBufHdrUnlocked(buf);

            buf_state = old_buf_state;

            buf_state -= BUF_REFCOUNT_ONE;

            if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state, buf_state)) {
                break;
            }
        }

        /* Support the function LockBufferForCleanup() */
        if (buf_state & BM_PIN_COUNT_WAITER) {
            /*
             * Acquire the buffer header lock, re-check that there's a waiter.
             * Another backend could have unpinned this buffer, and already
             * woken up the waiter.  There's no danger of the buffer being
             * replaced after we unpinned it above, as it's pinned by the
             * waiter.
             */
            buf_state = LockBufHdr(buf);
            if ((buf_state & BM_PIN_COUNT_WAITER) && BUF_STATE_GET_REFCOUNT(buf_state) == 1) {
                /* we just released the last pin other than the waiter's */
                ThreadId wait_backend_pid = buf->wait_backend_pid;

                buf_state &= ~BM_PIN_COUNT_WAITER;
                UnlockBufHdr(buf, buf_state);
                ProcSendSignal(wait_backend_pid);
            } else {
                UnlockBufHdr(buf, buf_state);
            }
        }
        ForgetPrivateRefCountEntry(ref);
    }
}

/*
 * BufferSync -- Write out all dirty buffers in the pool.
 *
 * This is called at checkpoint time to write out all dirty shared buffers.
 * The checkpoint request flags should be passed in.  If CHECKPOINT_IMMEDIATE
 * is set, we disable delays between writes; if CHECKPOINT_IS_SHUTDOWN is
 * set, we write even unlogged buffers, which are otherwise skipped.  The
 * remaining flags currently have no effect here.
 */
static void BufferSync(int flags)
{
    uint32 buf_state;
    int buf_id;
    int num_to_scan;
    int num_spaces;
    int num_processed;
    int num_written;
    double bufferFlushPercent = CHKPT_LOG_PERCENT_INTERVAL;
    CkptTsStatus *per_ts_stat = NULL;
    Oid last_tsid;
    binaryheap *ts_heap = NULL;
    int i;
    uint32 mask = BM_DIRTY;
    WritebackContext wb_context;

    gstrace_entry(GS_TRC_ID_BufferSync);

    /* Make sure we can handle the pin inside SyncOneBuffer */
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    /*
     * Unless this is a shutdown checkpoint, we write only permanent, dirty
     * buffers.  But at shutdown or end of recovery, we write all dirty buffers.
     */
    if (!((uint32)flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY))) {
        mask |= BM_PERMANENT;
    }

    /*
     * Loop over all buffers, and mark the ones that need to be written with
     * BM_CHECKPOINT_NEEDED.  Count them as we go (num_to_scan), so that we
     * can estimate how much work needs to be done.
     *
     * This allows us to write only those pages that were dirty when the
     * checkpoint began, and not those that get dirtied while it proceeds.
     * Whenever a page with BM_CHECKPOINT_NEEDED is written out, either by us
     * later in this function, or by normal backends or the bgwriter cleaning
     * scan, the flag is cleared.  Any buffer dirtied after this point won't
     * have the flag set.
     *
     * Note that if we fail to write some buffer, we may leave buffers with
     * BM_CHECKPOINT_NEEDED still set.	This is OK since any such buffer would
     * certainly need to be written for the next checkpoint attempt, too.
     */
    num_to_scan = 0;
    for (buf_id = 0; buf_id < TOTAL_BUFFER_NUM; buf_id++) {
        BufferDesc *buf_desc = GetBufferDescriptor(buf_id);

        /*
         * Header spinlock is enough to examine BM_DIRTY, see comment in
         * SyncOneBuffer.
         */
        buf_state = LockBufHdr(buf_desc);
        if ((buf_state & mask) == mask) {
            CkptSortItem *item = NULL;
            buf_state |= BM_CHECKPOINT_NEEDED;
            item = &g_instance.ckpt_cxt_ctl->CkptBufferIds[num_to_scan++];
            item->buf_id = buf_id;
            item->tsId = buf_desc->tag.rnode.spcNode;
            item->relNode = buf_desc->tag.rnode.relNode;
            item->bucketNode = buf_desc->tag.rnode.bucketNode;
            item->forkNum = buf_desc->tag.forkNum;
            item->blockNum = buf_desc->tag.blockNum;
        }

        UnlockBufHdr(buf_desc, buf_state);
    }

    if (num_to_scan == 0) {
        gstrace_exit(GS_TRC_ID_BufferSync);
        return; /* nothing to do */
    }

    WritebackContextInit(&wb_context, &u_sess->attr.attr_storage.checkpoint_flush_after);

    TRACE_POSTGRESQL_BUFFER_SYNC_START(g_instance.attr.attr_storage.NBuffers, num_to_scan);

    /*
     * Sort buffers that need to be written to reduce the likelihood of random
     * IO. The sorting is also important for the implementation of balancing
     * writes between tablespaces. Without balancing writes we'd potentially
     * end up writing to the tablespaces one-by-one; possibly overloading the
     * underlying system.
     */
    qsort(g_instance.ckpt_cxt_ctl->CkptBufferIds, num_to_scan, sizeof(CkptSortItem), ckpt_buforder_comparator);
    num_spaces = 0;

    /*
     * Allocate progress status for each tablespace with buffers that need to
     * be flushed. This requires the to-be-flushed array to be sorted.
     */
    last_tsid = InvalidOid;
    for (i = 0; i < num_to_scan; i++) {
        CkptTsStatus *s = NULL;
        Oid cur_tsid;

        cur_tsid = g_instance.ckpt_cxt_ctl->CkptBufferIds[i].tsId;

        /*
         * Grow array of per-tablespace status structs, every time a new
         * tablespace is found.
         */
        if (last_tsid == InvalidOid || last_tsid != cur_tsid) {
            Size sz;
            errno_t rc = EOK;

            num_spaces++;

            /*
             * Not worth adding grow-by-power-of-2 logic here - even with a
             * few hundred tablespaces this should be fine.
             */
            sz = sizeof(CkptTsStatus) * num_spaces;

            per_ts_stat = (per_ts_stat == NULL) ? (CkptTsStatus *)palloc(sz) :
                (CkptTsStatus *)repalloc(per_ts_stat, sz);

            s = &per_ts_stat[num_spaces - 1];
            rc = memset_s(s, sizeof(*s), 0, sizeof(*s));
            securec_check(rc, "\0", "\0");
            s->tsId = cur_tsid;

            /*
             * The first buffer in this tablespace. As CkptBufferIds is sorted
             * by tablespace all (s->num_to_scan) buffers in this tablespace
             * will follow afterwards.
             */
            s->index = i;

            /*
             * progress_slice will be determined once we know how many buffers
             * are in each tablespace, i.e. after this loop.
             */
            last_tsid = cur_tsid;
        } else {
            s = &per_ts_stat[num_spaces - 1];
        }

        s->num_to_scan++;
    }

    Assert(num_spaces > 0);

    /*
     * Build a min-heap over the write-progress in the individual tablespaces,
     * and compute how large a portion of the total progress a single
     * processed buffer is.
     */
    ts_heap = binaryheap_allocate(num_spaces, ts_ckpt_progress_comparator, NULL);

    for (i = 0; i < num_spaces; i++) {
        CkptTsStatus *ts_stat = &per_ts_stat[i];

        ts_stat->progress_slice = (float8)num_to_scan / ts_stat->num_to_scan;

        binaryheap_add_unordered(ts_heap, PointerGetDatum(ts_stat));
    }

    binaryheap_build(ts_heap);

    /*
     * Iterate through to-be-checkpointed buffers and write the ones (still)
     * marked with BM_CHECKPOINT_NEEDED. The writes are balanced between
     * tablespaces; otherwise the sorting would lead to only one tablespace
     * receiving writes at a time, making inefficient use of the hardware.
     */
    num_processed = 0;
    num_written = 0;
    while (!binaryheap_empty(ts_heap)) {
        BufferDesc *buf_desc = NULL;
        CkptTsStatus *ts_stat = (CkptTsStatus *)DatumGetPointer(binaryheap_first(ts_heap));

        buf_id = g_instance.ckpt_cxt_ctl->CkptBufferIds[ts_stat->index].buf_id;
        Assert(buf_id != -1);

        buf_desc = GetBufferDescriptor(buf_id);

        num_processed++;

        /*
         * We don't need to acquire the lock here, because we're only looking
         * at a single bit. It's possible that someone else writes the buffer
         * and clears the flag right after we check, but that doesn't matter
         * since SyncOneBuffer will then do nothing.  However, there is a
         * further race condition: it's conceivable that between the time we
         * examine the bit here and the time SyncOneBuffer acquires the lock,
         * someone else not only wrote the buffer but replaced it with another
         * page and dirtied it.  In that improbable case, SyncOneBuffer will
         * write the buffer though we didn't need to.  It doesn't seem worth
         * guarding against this, though.
         */
        if (pg_atomic_read_u32(&buf_desc->state) & BM_CHECKPOINT_NEEDED) {
            if (SyncOneBuffer(buf_id, false, &wb_context) & BUF_WRITTEN) {
                TRACE_POSTGRESQL_BUFFER_SYNC_WRITTEN(buf_id);
                u_sess->stat_cxt.BgWriterStats->m_buf_written_checkpoints++;
                num_written++;
            }
        }

        /*
         * Measure progress independent of actually having to flush the buffer
         * - otherwise writing become unbalanced.
         */
        ts_stat->progress += ts_stat->progress_slice;
        ts_stat->num_scanned++;
        ts_stat->index++;

        /* Have all the buffers from the tablespace been processed? */
        if (ts_stat->num_scanned == ts_stat->num_to_scan) {
            (void)binaryheap_remove_first(ts_heap);
        } else {
            /* update heap with the new progress */
            binaryheap_replace_first(ts_heap, PointerGetDatum(ts_stat));
        }

        double progress = (double)num_processed / num_to_scan;
        /*
         * Sleep to throttle our I/O rate.
         */
        CheckpointWriteDelay(flags, progress);
        if (((uint32)flags & CHECKPOINT_IS_SHUTDOWN) && progress >= bufferFlushPercent && !IsInitdb) {
            /* print warning log and increase counter if flushed percent exceed threshold */
            ereport(WARNING, (errmsg("full checkpoint mode, shuting down, wait for dirty page flush, remain num:%d",
                num_to_scan - num_processed)));
            bufferFlushPercent += CHKPT_LOG_PERCENT_INTERVAL;
        }
    }

    /* issue all pending flushes */
    IssuePendingWritebacks(&wb_context);

    pfree(per_ts_stat);
    per_ts_stat = NULL;
    binaryheap_free(ts_heap);

    /*
     * Update checkpoint statistics. As noted above, this doesn't include
     * buffers written by other backends or bgwriter scan.
     */
    t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written += num_written;

    TRACE_POSTGRESQL_BUFFER_SYNC_DONE(g_instance.attr.attr_storage.NBuffers, num_written, num_to_scan);
    gstrace_exit(GS_TRC_ID_BufferSync);
}
/*
 * BgBufferSync -- Write out some dirty buffers in the pool.
 *
 * This is called periodically by the background writer process.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.	(This happens if the strategy clock sweep
 * has been "lapped" and no buffer allocations have occurred recently,
 * or if the bgwriter has been effectively disabled by setting
 * u_sess->attr.attr_storage.bgwriter_lru_maxpages to 0.)
 */
bool BgBufferSync(WritebackContext *wb_context)
{
    /* info obtained from freelist.c */
    int strategy_buf_id;
    uint32 strategy_passes;
    uint32 recent_alloc;

    /* Potentially these could be tunables, but for now, not */
    const float smoothing_samples = 16;
    const float scan_whole_pool_milliseconds = 120000.0;

    /* Used to compute how far we scan ahead */
    long strategy_delta;
    int bufs_to_lap;
    int bufs_ahead;
    float scans_per_alloc;
    int reusable_buffers_est;
    int upcoming_alloc_est;
    int min_scan_buffers;

    /* Variables for the scanning loop proper */
    int num_to_scan;
    int num_written;
    int reusable_buffers;

    /* Variables for final smoothed_density update */
    long new_strategy_delta;
    uint32 new_recent_alloc;

    gstrace_entry(GS_TRC_ID_BgBufferSync);

    /*
     * Find out where the freelist clock sweep currently is, and how many
     * buffer allocations have happened since our last call.
     */
    strategy_buf_id = StrategySyncStart(&strategy_passes, &recent_alloc);

    /* Report buffer alloc counts to pgstat */
    u_sess->stat_cxt.BgWriterStats->m_buf_alloc += recent_alloc;

    /*
     * If we're not running the LRU scan, just stop after doing the stats
     * stuff.  We mark the saved state invalid so that we can recover sanely
     * if LRU scan is turned back on later.
     */
    if (u_sess->attr.attr_storage.bgwriter_lru_maxpages <= 0) {
        t_thrd.storage_cxt.saved_info_valid = false;
        gstrace_exit(GS_TRC_ID_BgBufferSync);
        return true;
    }

    /*
     * Compute strategy_delta = how many buffers have been scanned by the
     * clock sweep since last time.  If first time through, assume none. Then
     * see if we are still ahead of the clock sweep, and if so, how many
     * buffers we could scan before we'd catch up with it and "lap" it. Note:
     * weird-looking coding of xxx_passes comparisons are to avoid bogus
     * behavior when the passes counts wrap around.
     */
    if (t_thrd.storage_cxt.saved_info_valid) {
        int32 passes_delta = strategy_passes - t_thrd.storage_cxt.prev_strategy_passes;

        strategy_delta = strategy_buf_id - t_thrd.storage_cxt.prev_strategy_buf_id;
        strategy_delta += (long)passes_delta * NORMAL_SHARED_BUFFER_NUM;

        Assert(strategy_delta >= 0);

        if ((int32)(t_thrd.storage_cxt.next_passes - strategy_passes) > 0) {
            /* we're one pass ahead of the strategy point */
            bufs_to_lap = strategy_buf_id - t_thrd.storage_cxt.next_to_clean;
#ifdef BGW_DEBUG
            ereport(DEBUG2, (errmsg("bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
                                    t_thrd.storage_cxt.next_passes, t_thrd.storage_cxt.next_to_clean, strategy_passes,
                                    strategy_buf_id, strategy_delta, bufs_to_lap)));
#endif
        } else if (t_thrd.storage_cxt.next_passes == strategy_passes &&
                   t_thrd.storage_cxt.next_to_clean >= strategy_buf_id) {
            /* on same pass, but ahead or at least not behind */
            bufs_to_lap = NORMAL_SHARED_BUFFER_NUM - (t_thrd.storage_cxt.next_to_clean - strategy_buf_id);
#ifdef BGW_DEBUG
            ereport(DEBUG2, (errmsg("bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
                                    t_thrd.storage_cxt.next_passes, t_thrd.storage_cxt.next_to_clean, strategy_passes,
                                    strategy_buf_id, strategy_delta, bufs_to_lap)));
#endif
        } else {
            /*
             * We're behind, so skip forward to the strategy point and start
             * cleaning from there.
             */
#ifdef BGW_DEBUG
            ereport(DEBUG2,
                    (errmsg("bgwriter behind: bgw %u-%u strategy %u-%u delta=%ld", t_thrd.storage_cxt.next_passes,
                            t_thrd.storage_cxt.next_to_clean, strategy_passes, strategy_buf_id, strategy_delta)));
#endif
            t_thrd.storage_cxt.next_to_clean = strategy_buf_id;
            t_thrd.storage_cxt.next_passes = strategy_passes;
            bufs_to_lap = NORMAL_SHARED_BUFFER_NUM;
        }
    } else {
        /*
         * Initializing at startup or after LRU scanning had been off. Always
         * start at the strategy point.
         */
#ifdef BGW_DEBUG
        ereport(DEBUG2, (errmsg("bgwriter initializing: strategy %u-%u", strategy_passes, strategy_buf_id)));
#endif
        strategy_delta = 0;
        t_thrd.storage_cxt.next_to_clean = strategy_buf_id;
        t_thrd.storage_cxt.next_passes = strategy_passes;
        bufs_to_lap = NORMAL_SHARED_BUFFER_NUM;
    }

    /* Update saved info for next time */
    t_thrd.storage_cxt.prev_strategy_buf_id = strategy_buf_id;
    t_thrd.storage_cxt.prev_strategy_passes = strategy_passes;
    t_thrd.storage_cxt.saved_info_valid = true;

    /*
     * Compute how many buffers had to be scanned for each new allocation, ie,
     * 1/density of reusable buffers, and track a moving average of that.
     *
     * If the strategy point didn't move, we don't update the density estimate
     */
    if (strategy_delta > 0 && recent_alloc > 0) {
        scans_per_alloc = (float)strategy_delta / (float)recent_alloc;
        t_thrd.storage_cxt.smoothed_density += (scans_per_alloc - t_thrd.storage_cxt.smoothed_density) /
                                               smoothing_samples;
    }

    /*
     * Estimate how many reusable buffers there are between the current
     * strategy point and where we've scanned ahead to, based on the smoothed
     * density estimate.
     */
    bufs_ahead = NORMAL_SHARED_BUFFER_NUM - bufs_to_lap;
    reusable_buffers_est = (int)(bufs_ahead / t_thrd.storage_cxt.smoothed_density);

    /*
     * Track a moving average of recent buffer allocations.  Here, rather than
     * a true average we want a fast-attack, slow-decline behavior: we
     * immediately follow any increase.
     */
    if (t_thrd.storage_cxt.smoothed_alloc <= (float)recent_alloc) {
        t_thrd.storage_cxt.smoothed_alloc = recent_alloc;
    } else {
        t_thrd.storage_cxt.smoothed_alloc += ((float)recent_alloc - t_thrd.storage_cxt.smoothed_alloc) /
                                             smoothing_samples;
    }

    /* Scale the estimate by a GUC to allow more aggressive tuning. */
    upcoming_alloc_est = (int)(t_thrd.storage_cxt.smoothed_alloc * u_sess->attr.attr_storage.bgwriter_lru_multiplier);

    /*
     * If recent_alloc remains at zero for many cycles, smoothed_alloc will
     * eventually underflow to zero, and the underflows produce annoying
     * kernel warnings on some platforms.  Once upcoming_alloc_est has gone to
     * zero, there's no point in tracking smaller and smaller values of
     * smoothed_alloc, so just reset it to exactly zero to avoid this
     * syndrome.  It will pop back up as soon as recent_alloc increases.
     */
    if (upcoming_alloc_est == 0) {
        t_thrd.storage_cxt.smoothed_alloc = 0;
    }

    /*
     * Even in cases where there's been little or no buffer allocation
     * activity, we want to make a small amount of progress through the buffer
     * cache so that as many reusable buffers as possible are clean after an
     * idle period.
     *
     * (scan_whole_pool_milliseconds / u_sess->attr.attr_storage.BgWriterDelay) computes how many times
     * the BGW will be called during the scan_whole_pool time; slice the
     * buffer pool into that many sections.
     */
    min_scan_buffers = (int)(TOTAL_BUFFER_NUM /
                             (scan_whole_pool_milliseconds / u_sess->attr.attr_storage.BgWriterDelay));

    if (upcoming_alloc_est < (min_scan_buffers + reusable_buffers_est)) {
#ifdef BGW_DEBUG
        ereport(DEBUG2, (errmsg("bgwriter: alloc_est=%d too small, using min=%d + reusable_est=%d", upcoming_alloc_est,
                                min_scan_buffers, reusable_buffers_est)));
#endif
        upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
    }

    /*
     * Now write out dirty reusable buffers, working forward from the
     * next_to_clean point, until we have lapped the strategy scan, or cleaned
     * enough buffers to match our estimate of the next cycle's allocation
     * requirements, or hit the u_sess->attr.attr_storage.bgwriter_lru_maxpages limit.
     */
    num_to_scan = bufs_to_lap;
    num_written = 0;
    reusable_buffers = reusable_buffers_est;

#ifndef ENABLE_LITE_MODE
    if (AioCompltrIsReady() == true && g_instance.attr.attr_storage.enable_adio_function) {
        /* Execute the LRU scan
         *
         * The ADIO equivalent starts the pool far enough to satisfy the
         * upcoming_alloc_est MAX_BACKWRITE_SCAN controls how often
         * PageRangeBackWrite comes up for air.
         * There is no delay here so the writes counted may not have been done yet.
         */
        while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est) {
            int scan_this_round;
            int wrote_this_round = 0;
            int reusable_this_round = 0;

            scan_this_round = ((num_to_scan - u_sess->attr.attr_storage.backwrite_quantity) > 0)
                                    ? u_sess->attr.attr_storage.backwrite_quantity
                                    : num_to_scan;

            /* Write the range of buffers concurrently */
            PageRangeBackWrite(t_thrd.storage_cxt.next_to_clean, scan_this_round, 0, NULL, &wrote_this_round,
                               &reusable_this_round);

            /*  anywary we should change next_to_clean and num_to_scan first, make the value of num_to_scan correct
             *
             * Calculate next buffer range starting point
             */
            t_thrd.storage_cxt.next_to_clean += scan_this_round;
            if (t_thrd.storage_cxt.next_to_clean >= TOTAL_BUFFER_NUM) {
                t_thrd.storage_cxt.next_to_clean -= TOTAL_BUFFER_NUM;
            }
            num_to_scan -= scan_this_round;

            if (wrote_this_round) {
                reusable_buffers += (reusable_this_round + wrote_this_round);
                num_written += wrote_this_round;
                /*
                 * Stop when the configurable quota is met.
                 */
                if (num_written >= u_sess->attr.attr_storage.bgwriter_lru_maxpages) {
                    u_sess->stat_cxt.BgWriterStats->m_maxwritten_clean += num_written;
                    break;
                }
            } else if (reusable_this_round) {
                reusable_buffers += reusable_this_round;
            } else {
            }
        }
    } else {
#endif
        /* Make sure we can handle the pin inside SyncOneBuffer */
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
        /* Execute the LRU scan */
        while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est) {
            uint32 sync_state = SyncOneBuffer(t_thrd.storage_cxt.next_to_clean, true, wb_context);

            if (++t_thrd.storage_cxt.next_to_clean >= TOTAL_BUFFER_NUM) {
                t_thrd.storage_cxt.next_to_clean = 0;
                t_thrd.storage_cxt.next_passes++;
            }
            num_to_scan--;

            if (sync_state & BUF_WRITTEN) {
                reusable_buffers++;
                if (++num_written >= u_sess->attr.attr_storage.bgwriter_lru_maxpages) {
                    u_sess->stat_cxt.BgWriterStats->m_maxwritten_clean++;
                    break;
                }
            } else if (sync_state & BUF_REUSABLE) {
                reusable_buffers++;
            } else {
            }
        }
#ifndef ENABLE_LITE_MODE
    }
#endif

    u_sess->stat_cxt.BgWriterStats->m_buf_written_clean += num_written;

#ifdef BGW_DEBUG
    ereport(DEBUG1, (errmsg("bgwriter: recent_alloc=%u smoothed=%.2f delta=%ld ahead=%d density=%.2f reusable_est=%d "
                            "upcoming_est=%d scanned=%d wrote=%d reusable=%d",
                            recent_alloc, t_thrd.storage_cxt.smoothed_alloc, strategy_delta, bufs_ahead,
                            t_thrd.storage_cxt.smoothed_density, reusable_buffers_est, upcoming_alloc_est,
                            bufs_to_lap - num_to_scan, num_written, reusable_buffers - reusable_buffers_est)));
#endif

    /*
     * Consider the above scan as being like a new allocation scan.
     * Characterize its density and update the smoothed one based on it. This
     * effectively halves the moving average period in cases where both the
     * strategy and the background writer are doing some useful scanning,
     * which is helpful because a long memory isn't as desirable on the
     * density estimates.
     */
    new_strategy_delta = bufs_to_lap - num_to_scan;
    new_recent_alloc = reusable_buffers - reusable_buffers_est;
    if (new_strategy_delta > 0 && new_recent_alloc > 0) {
        scans_per_alloc = (float)new_strategy_delta / (float)new_recent_alloc;
        t_thrd.storage_cxt.smoothed_density += (scans_per_alloc - t_thrd.storage_cxt.smoothed_density) /
                                               smoothing_samples;

#ifdef BGW_DEBUG
        ereport(DEBUG2,
                (errmsg("bgwriter: cleaner density alloc=%u scan=%ld density=%.2f new smoothed=%.2f", new_recent_alloc,
                        new_strategy_delta, scans_per_alloc, t_thrd.storage_cxt.smoothed_density)));
#endif
    }

    gstrace_exit(GS_TRC_ID_BgBufferSync);
    /* Return true if OK to hibernate */
    return (bufs_to_lap == 0 && recent_alloc == 0);
}

const int CONDITION_LOCK_RETRY_TIMES = 5;
bool SyncFlushOneBuffer(int buf_id, bool get_condition_lock)
{
    BufferDesc *buf_desc = GetBufferDescriptor(buf_id);
    /*
     * Pin it, share-lock it, write it.  (FlushBuffer will do nothing if the
     * buffer is clean by the time we've locked it.)
     */
    PinBuffer_Locked(buf_desc);

    if (dw_enabled() && get_condition_lock) {
        /*
         * We must use a conditional lock acquisition here to avoid deadlock. If
         * page_writer and double_write are enabled, only page_writer is allowed to
         * flush the buffers. So the backends (BufferAlloc, FlushRelationBuffers,
         * FlushDatabaseBuffers) are not allowed to flush the buffers, instead they
         * will just wait for page_writer to flush the required buffer. In some cases
         * (for example, btree split, heap_multi_insert), BufferAlloc will be called
         * with holding exclusive lock on another buffer. So if we try to acquire
         * the shared lock directly here (page_writer), it will block unconditionally
         * and the backends will be blocked on the page_writer to flush the buffer,
         * resulting in deadlock.
         */
        int retry_times = 0;
        int i = 0;
        Buffer queue_head_buffer = get_dirty_page_queue_head_buffer();
        if (!BufferIsInvalid(queue_head_buffer) && (queue_head_buffer - 1 == buf_id)) {
            retry_times = CONDITION_LOCK_RETRY_TIMES;
        }
        for (;;) {
            if (!LWLockConditionalAcquire(buf_desc->content_lock, LW_SHARED)) {
                i++;
                if (i >= retry_times) {
                    UnpinBuffer(buf_desc, true);
                    return false;
                }
                (void)sched_yield();
                continue;
            }
            break;
        }
    } else {
        (void)LWLockAcquire(buf_desc->content_lock, LW_SHARED);
    }

    if (IsSegmentBufferID(buf_id)) {
        Assert(IsSegmentPhysicalRelNode(buf_desc->tag.rnode));
        SegFlushBuffer(buf_desc, NULL);
    } else {
        FlushBuffer(buf_desc, NULL);
    }

    LWLockRelease(buf_desc->content_lock);
    return true;
}

/*
 * SyncOneBuffer -- process a single buffer during syncing.
 *
 * If skip_recently_used is true, we don't write currently-pinned buffers, nor
 * buffers marked recently used, as these are not replacement candidates.
 *
 * Returns a bitmask containing the following flag bits:
 *	BUF_WRITTEN: we wrote the buffer.
 *	BUF_REUSABLE: buffer is available for replacement, ie, it has
 *		pin count 0 and usage count 0.
 *
 * (BUF_WRITTEN could be set in error if FlushBuffers finds the buffer clean
 * after locking it, but we don't care all that much.)
 *
 * Note: caller must have done ResourceOwnerEnlargeBuffers.
 */
uint32 SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext* wb_context, bool get_condition_lock)
{
    BufferDesc *buf_desc = GetBufferDescriptor(buf_id);
    uint32 result = 0;
    BufferTag tag;
    uint32 buf_state;
    /*
     * Check whether buffer needs writing.
     *
     * We can make this check without taking the buffer content lock so long
     * as we mark pages dirty in access methods *before* logging changes with
     * XLogInsert(): if someone marks the buffer dirty just after our check we
     * don't worry because our checkpoint.redo points before log record for
     * upcoming changes and so we are not required to write such dirty buffer.
     */
    buf_state = LockBufHdr(buf_desc);
    if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 && BUF_STATE_GET_USAGECOUNT(buf_state) == 0) {
        result |= BUF_REUSABLE;
    } else if (skip_recently_used) {
        /* Caller told us not to write recently-used buffers */
        UnlockBufHdr(buf_desc, buf_state);
        return result;
    }

    if (!(buf_state & BM_VALID) || !(buf_state & BM_DIRTY)) {
        /* It's clean, so nothing to do */
        UnlockBufHdr(buf_desc, buf_state);
        return result;
    }

    if (!SyncFlushOneBuffer(buf_id, get_condition_lock)) {
        return (result | BUF_SKIPPED);
    }

    tag = buf_desc->tag;
    if (buf_desc->seg_fileno != EXTENT_INVALID) {
        SegmentCheck(XLOG_NEED_PHYSICAL_LOCATION(buf_desc->tag.rnode));
        SegmentCheck(buf_desc->seg_blockno != InvalidBlockNumber);
        SegmentCheck(ExtentTypeIsValid(buf_desc->seg_fileno));
        tag.rnode.relNode = buf_desc->seg_fileno;
        tag.blockNum = buf_desc->seg_blockno;
    }
    UnpinBuffer(buf_desc, true);

    ScheduleBufferTagForWriteback(wb_context, &tag);

    return (result | BUF_WRITTEN);
}

void SyncOneBufferForExtremRto(RedoBufferInfo *bufferinfo)
{
    if (dw_enabled()) {
        /* double write */
    }

    FlushBuffer((void *)bufferinfo, NULL);

    return;
}
/*
 * Flush a previously, shared or exclusively, locked and pinned buffer to the
 * OS.
 */
void FlushOneBuffer(Buffer buffer)
{
    BufferDesc *buf_desc = NULL;

    /* currently not needed, but no fundamental reason not to support */
    Assert(!BufferIsLocal(buffer));

    Assert(BufferIsPinned(buffer));

    buf_desc = GetBufferDescriptor(buffer - 1);
    /* unlogged table won't need double write protection */
    FlushBuffer(buf_desc, NULL);
}

/*
 *		AtEOXact_Buffers - clean up at end of transaction.
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
void AtEOXact_Buffers(bool isCommit)
{
    CheckForBufferLeaks();

    AtEOXact_LocalBuffers(isCommit);

    Assert(t_thrd.storage_cxt.PrivateRefCountOverflowed == 0);
}

/*
 * Initialize access to shared buffer pool
 *
 * This is called during backend startup (whether standalone or under the
 * postmaster).  It sets up for this backend's access to the already-existing
 * buffer pool.
 *
 * NB: this is called before InitProcess(), so we do not have a PGPROC and
 * cannot do LWLockAcquire; hence we can't actually access stuff in
 * shared memory yet.  We are only initializing local data here.
 * (See also InitBufferPoolBackend)
 */
void InitBufferPoolAccess(void)
{
    HASHCTL hash_ctl;
    errno_t rc = EOK;

    rc = memset_s(t_thrd.storage_cxt.PrivateRefCountArray, REFCOUNT_ARRAY_ENTRIES * sizeof(PrivateRefCountEntry), 0,
                  REFCOUNT_ARRAY_ENTRIES * sizeof(PrivateRefCountEntry));
    securec_check(rc, "\0", "\0");

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(int32);
    hash_ctl.entrysize = REFCOUNT_ARRAY_ENTRIES * sizeof(PrivateRefCountEntry);
    hash_ctl.hash = oid_hash; /* a bit more efficient than tag_hash */

    t_thrd.storage_cxt.PrivateRefCountHash = hash_create("PrivateRefCount", 100, &hash_ctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * InitBufferPoolBackend --- second-stage initialization of a new backend
 *
 * This is called after we have acquired a PGPROC and so can safely get
 * LWLocks.  We don't currently need to do anything at this stage ...
 * except register a shmem-exit callback.  AtProcExit_Buffers needs LWLock
 * access, and thereby has to be called at the corresponding phase of
 * backend shutdown.
 */
void InitBufferPoolBackend(void)
{
    on_shmem_exit(AtProcExit_Buffers, 0);
}

/*
 * During backend exit, ensure that we released all shared-buffer locks and
 * assert that we have no remaining pins.
 */
void AtProcExit_Buffers(int code, Datum arg)
{
    AbortAsyncListIO();
    AbortBufferIO();
    UnlockBuffers();

    CheckForBufferLeaks();

    /* localbuf.c needs a chance too */
    AtProcExit_LocalBuffers();
}

int GetThreadBufferLeakNum(void)
{
    int refCountErrors = 0;
    PrivateRefCountEntry *res = NULL;

    /* check the array */
    for (int i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++) {
        res = &t_thrd.storage_cxt.PrivateRefCountArray[i];

        if (res->buffer != InvalidBuffer) {
            PrintBufferLeakWarning(res->buffer);
            refCountErrors++;
        }
    }

    /* if neccessary search the hash */
    if (t_thrd.storage_cxt.PrivateRefCountOverflowed) {
        HASH_SEQ_STATUS hstat;
        hash_seq_init(&hstat, t_thrd.storage_cxt.PrivateRefCountHash);
        while ((res = (PrivateRefCountEntry *)hash_seq_search(&hstat)) != NULL) {
            PrintBufferLeakWarning(res->buffer);
            refCountErrors++;
        }
    }
    return refCountErrors;
}

/*
 *		CheckForBufferLeaks - ensure this backend holds no buffer pins
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
static void CheckForBufferLeaks(void)
{
#ifdef USE_ASSERT_CHECKING
    if (RecoveryInProgress()) {
        return;
    }
    Assert(GetThreadBufferLeakNum() == 0);
#endif
}

/*
 * Helper routine to issue warnings when a buffer is unexpectedly pinned
 */
void PrintBufferLeakWarning(Buffer buffer)
{
    volatile BufferDesc *buf = NULL;
    int32 loccount;
    char *path = NULL;
    BackendId backend;
    uint32 buf_state;

    Assert(BufferIsValid(buffer));
    if (BufferIsLocal(buffer)) {
        buf = &u_sess->storage_cxt.LocalBufferDescriptors[-buffer - 1];
        loccount = u_sess->storage_cxt.LocalRefCount[-buffer - 1];
        backend = BackendIdForTempRelations;
    } else {
        buf = GetBufferDescriptor(buffer - 1);
        loccount = GetPrivateRefCount(buffer);
        backend = InvalidBackendId;
    }

    /* theoretically we should lock the bufhdr here */
    path = relpathbackend(((BufferDesc *)buf)->tag.rnode, backend, ((BufferDesc *)buf)->tag.forkNum);
    buf_state = pg_atomic_read_u32(&buf->state);
    ereport(WARNING, (errmsg("buffer refcount leak: [%03d] "
                             "(rel=%s, blockNum=%u, flags=0x%x, refcount=%u %d)",
                             buffer, path, buf->tag.blockNum, buf_state & BUF_FLAG_MASK,
                             BUF_STATE_GET_REFCOUNT(buf_state), loccount)));
    pfree(path);
}

/*
 * CheckPointBuffers
 *
 * Flush all dirty blocks in buffer pool to disk at checkpoint time.
 *
 * Note: temporary relations do not participate in checkpoints, so they don't
 * need to be flushed.
 *
 * If the enable_incremental_checkpoint is on, first case, the doFullCheckpoint
 * is true, need wait all dirty blocks flush finish. second case, the
 * doFullCheckpoint is false, don't need flush any dirty blocks.
 */
void CheckPointBuffers(int flags, bool doFullCheckpoint)
{
    TRACE_POSTGRESQL_BUFFER_CHECKPOINT_START(flags);
    gstrace_entry(GS_TRC_ID_CheckPointBuffers);
    t_thrd.xlog_cxt.CheckpointStats->ckpt_write_t = GetCurrentTimestamp();

    /*
     * If the enable_incremental_checkpoint is off, checkpoint thread call BufferSync flush all dirty
     * to data file. if IsBootstrapProcessingMode or pagewriter thread is not started also need call
     * BufferSync to flush dirty page.
     */
    if (USE_CKPT_THREAD_SYNC) {
        BufferSync(flags);
    } else if (ENABLE_INCRE_CKPT && doFullCheckpoint) {
        long waitCount = 0;
        /*
         * If the enable_incremental_checkpoint is on, but doFullCheckpoint is true (full checkpoint),
         * checkpoint thread don't need flush dirty page, but need wait pagewriter thread flush given
         * dirty page num.
         */
        for (;;) {
            pg_memory_barrier();
            if ((pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head) >=
                 pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc)) ||
                get_dirty_page_num() == 0) {
                break;
            } else {
                /* sleep 1 ms wait the dirty page flush */
                long sleepTime = ONE_MILLISECOND * MILLISECOND_TO_MICROSECOND;
                pg_usleep(sleepTime);
                if (((uint32)flags & CHECKPOINT_IS_SHUTDOWN) && !IsInitdb) {
                    /*
                     * since we use sleep time as counter so there will be some error in calculate the interval,
                     * but it doesn't mater cause we don't need a precise counter.
                     */
                    waitCount += sleepTime;
                    if (waitCount >= CHKPT_LOG_TIME_INTERVAL) {
                        /* print warning log and reset counter if waitting time exceed threshold */
                        ereport(WARNING, (errmsg("incremental checkpoint mode, shuting down, "
                            "wait for dirty page flush, remain num:%u",
                            g_instance.ckpt_cxt_ctl->actual_dirty_page_num)));
                        waitCount = 0;
                    }
                }
            }
        }
    }
    g_instance.ckpt_cxt_ctl->flush_all_dirty_page = false;

    t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_t = GetCurrentTimestamp();
    TRACE_POSTGRESQL_BUFFER_CHECKPOINT_SYNC_START();
    /*
     * If the enable_incremental_checkpoint is off, checkpoint thread call ProcessSyncRequests handle the sync,
     * if IsBootstrapProcessingMode or pagewriter thread is not started also need call ProcessSyncRequests.
     */
    if (USE_CKPT_THREAD_SYNC) {
        ProcessSyncRequests();
    } else {
        /* incremental checkpoint, requeset the pagewriter handle the file sync */
        PageWriterSync();
        dw_truncate();
    }

    /* When finish shutdown checkpoint, pagewriter thread can exit after finish the file sync. */
    if (((uint32)flags & CHECKPOINT_IS_SHUTDOWN)) {
        g_instance.ckpt_cxt_ctl->page_writer_can_exit = true;
    }

    t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_end_t = GetCurrentTimestamp();
    TRACE_POSTGRESQL_BUFFER_CHECKPOINT_DONE();

    gstrace_exit(GS_TRC_ID_CheckPointBuffers);
}

/*
 * Do whatever is needed to prepare for commit at the bufmgr and smgr levels
 */
void BufmgrCommit(void)
{
    /* Nothing to do in bufmgr anymore... */
}

/*
 * BufferGetBlockNumber
 *		Returns the block number associated with a buffer.
 *
 * Note:
 *		Assumes that the buffer is valid and pinned, else the
 *		value may be obsolete immediately...
 */
FORCE_INLINE
BlockNumber BufferGetBlockNumber(Buffer buffer)
{
    volatile BufferDesc *buf_desc = NULL;

    Assert(BufferIsPinned(buffer));

    if (BufferIsLocal(buffer)) {
        buf_desc = &(u_sess->storage_cxt.LocalBufferDescriptors[-buffer - 1]);
    } else {
        buf_desc = GetBufferDescriptor(buffer - 1);
    }
    /* pinned, so OK to read tag without spinlock */
    return buf_desc->tag.blockNum;
}

/* Returns the relfilenode, fork number and block number associated with a buffer. */
void BufferGetTag(Buffer buffer, RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum)
{
    volatile BufferDesc *buf_desc = NULL;

    /* Do the same checks as BufferGetBlockNumber. */
    Assert(BufferIsPinned(buffer));
    if (BufferIsLocal(buffer)) {
        buf_desc = &(u_sess->storage_cxt.LocalBufferDescriptors[-buffer - 1]);
    } else {
        buf_desc = GetBufferDescriptor(buffer - 1);
    }

    /* pinned, so OK to read tag without spinlock */
    *rnode = ((BufferDesc *)buf_desc)->tag.rnode;
    *forknum = buf_desc->tag.forkNum;
    *blknum = buf_desc->tag.blockNum;
}

#define PG_STAT_TRACK_IO_TIMING(io_time, io_start) do { \
    INSTR_TIME_SET_CURRENT(io_time);                                            \
    INSTR_TIME_SUBTRACT(io_time, io_start);                                     \
    pgstat_count_buffer_write_time(INSTR_TIME_GET_MICROSEC(io_time));           \
    INSTR_TIME_ADD(u_sess->instr_cxt.pg_buffer_usage->blk_write_time, io_time); \
    pgstatCountBlocksWriteTime4SessionLevel(INSTR_TIME_GET_MICROSEC(io_time));  \
} while (0)

void GetFlushBufferInfo(void *buf, RedoBufferInfo *bufferinfo, uint32 *buf_state, ReadBufferMethod flushmethod)
{
    if (flushmethod == WITH_NORMAL_CACHE || flushmethod == WITH_LOCAL_CACHE) {
        BufferDesc *bufdesc = (BufferDesc *)buf;
        bufferinfo->blockinfo.rnode = bufdesc->tag.rnode;
        bufferinfo->blockinfo.forknum = bufdesc->tag.forkNum;
        bufferinfo->blockinfo.blkno = bufdesc->tag.blockNum;
        *buf_state = LockBufHdr(bufdesc);
        /*
         * Run PageGetLSN while holding header lock, since we don't have the
         * buffer locked exclusively in all cases.
         */
        /* To check if block content changes while flushing. - vadim 01/17/97 */
        *buf_state &= ~BM_JUST_DIRTIED;
        bufferinfo->lsn = (flushmethod == WITH_LOCAL_CACHE) ? LocalBufGetLSN(bufdesc) : BufferGetLSN(bufdesc);
        UnlockBufHdr(bufdesc, *buf_state);
        bufferinfo->buf = BufferDescriptorGetBuffer(bufdesc);
        bufferinfo->pageinfo.page = (flushmethod == WITH_LOCAL_CACHE) ? (Page)LocalBufHdrGetBlock(bufdesc)
                                                                        : (Page)BufHdrGetBlock(bufdesc);
        bufferinfo->pageinfo.pagesize = BufferGetPageSize(bufferinfo->buf);
    } else {
        *bufferinfo = *((RedoBufferInfo *)buf);
    }
}

/* encrypt page data before write buffer to page when reloption of enable_tde is on */
char* PageDataEncryptForBuffer(Page page, BufferDesc *bufdesc, bool is_segbuf)
{
    char *bufToWrite = NULL;
    TdeInfo tde_info = {0};

    if (bufdesc->encrypt) {
        TDE::TDEBufferCache::get_instance().search_cache(bufdesc->tag.rnode, &tde_info);
        if (strlen(tde_info.dek_cipher) == 0) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("page buffer get TDE buffer cache entry failed, RelFileNode is %u/%u/%u",
                       bufdesc->tag.rnode.spcNode, bufdesc->tag.rnode.dbNode, bufdesc->tag.rnode.relNode),
                errdetail("N/A"),
                errcause("TDE cache miss this key"),
                erraction("check cache status")));
        }
        bufToWrite = PageDataEncryptIfNeed(page, &tde_info, true, is_segbuf);
    } else {
        bufToWrite = (char*)page;
    }
    return bufToWrite;
}

/*
 * Physically write out a shared buffer.
 * NOTE: this actually just passes the buffer contents to the kernel; the
 * real write to disk won't happen until the kernel feels like it.  This
 * is okay from our point of view since we can redo the changes from WAL.
 * However, we will need to force the changes to disk via fsync before
 * we can checkpoint WAL.
 *
 * The caller must hold a pin on the buffer and have share-locked the
 * buffer contents.  (Note: a share-lock does not prevent updates of
 * hint bits in the buffer, so the page could change while the write
 * is in progress, but we assume that that will not invalidate the data
 * written.)
 *
 * If the caller has an smgr reference for the buffer's relation, pass it
 * as the second parameter.  If not, pass NULL.
 */
void FlushBuffer(void *buf, SMgrRelation reln, ReadBufferMethod flushmethod, bool skipFsync)
{
    bool logicalpage = false;
    ErrorContextCallback errcontext;
    instr_time io_start, io_time;
    Block bufBlock;
    char *bufToWrite = NULL;
    uint32 buf_state;
    RedoBufferInfo bufferinfo = {0};

    /*
     * Acquire the buffer's io_in_progress lock.  If StartBufferIO returns
     * false, then someone else flushed the buffer before we could, so we need
     * not do anything.
     */
    if (flushmethod == WITH_NORMAL_CACHE) {
        if (!StartBufferIO((BufferDesc *)buf, false))
            return;
    }

    /*
     * Set up error traceback if we are not pagewriter.
     * If we are a page writer, let thread's own callback handles the error.
     */
    if (t_thrd.role != PAGEWRITER_THREAD && t_thrd.role != BGWRITER) {
        errcontext.callback = shared_buffer_write_error_callback;
        errcontext.arg = (void *)buf;
        errcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &errcontext;
    }

    GetFlushBufferInfo(buf, &bufferinfo, &buf_state, flushmethod);

    /* Find smgr relation for buffer */
    if (reln == NULL || (IsValidColForkNum(bufferinfo.blockinfo.forknum)))
        reln = smgropen(bufferinfo.blockinfo.rnode, InvalidBackendId, GetColumnNum(bufferinfo.blockinfo.forknum));

    TRACE_POSTGRESQL_BUFFER_FLUSH_START(bufferinfo.blockinfo.forknum, bufferinfo.blockinfo.blkno,
                                        reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode);

    /*
     * Force XLOG flush up to buffer's LSN.  This implements the basic WAL
     * rule that log updates must hit disk before any of the data-file changes
     * they describe do.
     *
     * For data replication, standby maybe get page faster than xlog, we will
     * get an ERROR at XLogFlush. So for logical page, we report a WARNING
     * replace ERROR.
     */
    logicalpage = PageIsLogical((Block)bufferinfo.pageinfo.page);
    if (FORCE_FINISH_ENABLED) {
        update_max_page_flush_lsn(bufferinfo.lsn, t_thrd.proc_cxt.MyProcPid, false);
    }
    XLogWaitFlush(bufferinfo.lsn);

    /*
     * Now it's safe to write buffer to disk. Note that no one else should
     * have been able to write it while we were busy with log flushing because
     * we have the io_in_progress lock.
     */
    bufBlock = (Block)bufferinfo.pageinfo.page;

    BufferDesc *bufdesc = GetBufferDescriptor(bufferinfo.buf - 1);
    /* data encrypt */
    bufToWrite = PageDataEncryptForBuffer((Page)bufBlock, bufdesc);

    /* Segment-page storage will set checksum with physical block number later. */
    if (!IsSegmentFileNode(bufdesc->tag.rnode)) {
        /*
        * Update page checksum if desired.  Since we have only shared lock on the
        * buffer, other processes might be updating hint bits in it, so we must
        * copy the page to private storage if we do checksumming.
        */
        if (unlikely(bufToWrite != (char *)bufBlock))
            /* with data encrypt and page copyed  */
            PageSetChecksumInplace((Page)bufToWrite, bufferinfo.blockinfo.blkno);
        else
            /* without data encrypt and page not copyed  */
            bufToWrite = PageSetChecksumCopy((Page)bufToWrite, bufferinfo.blockinfo.blkno);
    }

    INSTR_TIME_SET_CURRENT(io_start);

    if (bufdesc->seg_fileno != EXTENT_INVALID) {
        /* FlushBuffer only used for data buffer, matedata buffer is flushed by SegFlushBuffer */
        SegmentCheck(!PageIsSegmentVersion(bufBlock) || PageIsNew(bufBlock));
        SegmentCheck(XLOG_NEED_PHYSICAL_LOCATION(bufdesc->tag.rnode));
        SegmentCheck(bufdesc->seg_blockno != InvalidBlockNumber);
        SegmentCheck(ExtentTypeIsValid(bufdesc->seg_fileno));

        if (unlikely(bufToWrite != (char *)bufBlock)) {
            PageSetChecksumInplace((Page)bufToWrite, bufdesc->seg_blockno);
        } else {
            bufToWrite = PageSetChecksumCopy((Page)bufToWrite, bufdesc->seg_blockno, true);
        }

        SegSpace* spc = spc_open(reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode, false);
        SegmentCheck(spc);
        RelFileNode fakenode = {
            .spcNode = spc->spcNode,
            .dbNode = spc->dbNode,
            .relNode = bufdesc->seg_fileno,
            .bucketNode = SegmentBktId,
            .opt = 0
        };
        seg_physical_write(spc, fakenode, bufferinfo.blockinfo.forknum, bufdesc->seg_blockno, bufToWrite, false);
    } else {
        SegmentCheck(!IsSegmentFileNode(bufdesc->tag.rnode));
        smgrwrite(reln, bufferinfo.blockinfo.forknum, bufferinfo.blockinfo.blkno, bufToWrite, skipFsync);
    }

    if (u_sess->attr.attr_common.track_io_timing) {
        PG_STAT_TRACK_IO_TIMING(io_time, io_start);
    } else {
        INSTR_TIME_SET_CURRENT(io_time);
        INSTR_TIME_SUBTRACT(io_time, io_start);
        pgstatCountBlocksWriteTime4SessionLevel(INSTR_TIME_GET_MICROSEC(io_time));
    }

    u_sess->instr_cxt.pg_buffer_usage->shared_blks_written++;

    ((BufferDesc *)buf)->lsn_on_disk = bufferinfo.lsn;

    /*
     * Mark the buffer as clean (unless BM_JUST_DIRTIED has become set) and
     * end the io_in_progress state.
     */
    if (flushmethod == WITH_NORMAL_CACHE) {
        TerminateBufferIO((BufferDesc *)buf, true, 0);
    }

    TRACE_POSTGRESQL_BUFFER_FLUSH_DONE(bufferinfo.blockinfo.forknum, bufferinfo.blockinfo.blkno,
                                       bufferinfo.blockinfo.rnode.spcNode, bufferinfo.blockinfo.rnode.dbNode,
                                       bufferinfo.blockinfo.rnode.relNode);

    /* Pop the error context stack, if it was set before */
    if (t_thrd.role != PAGEWRITER_THREAD && t_thrd.role != BGWRITER) {
        t_thrd.log_cxt.error_context_stack = errcontext.previous;
    }
}

/*
 * RelationGetNumberOfBlocks
 *		Determines the current number of pages in the relation.
 */
BlockNumber RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber fork_num, bool estimate)
{
    BlockNumber result = 0;
    /*
     * When this backend not init gtt storage
     * return 0
     */
    if (RELATION_IS_GLOBAL_TEMP(relation) &&
        !gtt_storage_attached(RelationGetRelid(relation))) {
        return result;
    }

    // Just return the relpages in pg_class for column-store relation.
    // Future: new interface should be implemented to calculate the number of blocks.
    if (RelationIsColStore(relation)) {
        return (BlockNumber)relation->rd_rel->relpages;
    }

    if (RELATION_CREATE_BUCKET(relation)) {
        Relation buckRel = NULL;

        oidvector *bucketlist = searchHashBucketByOid(relation->rd_bucketoid);

        if (estimate) {
            /*
             * Estimate size of relation using the first bucket rel, only used for planner.
             */
            buckRel = bucketGetRelation(relation, NULL, bucketlist->values[0]);
            result += smgrnblocks(buckRel->rd_smgr, fork_num) * bucketlist->dim1;
            bucketCloseRelation(buckRel);

            /*
             * If the result is suspiciously small,
             * we take the relpages from relation data instead.
             */
            if (result < ESTIMATED_MIN_BLOCKS) {
                result = relation->rd_rel->relpages;
            }
        } else {
            for (int i = 0; i < bucketlist->dim1; i++) {
                buckRel = bucketGetRelation(relation, NULL, bucketlist->values[i]);
                result += smgrnblocks(buckRel->rd_smgr, fork_num);
                bucketCloseRelation(buckRel);
            }
        }
        return result;
    } else {
        /* Open it at the smgr level if not already done */
        RelationOpenSmgr(relation);
        return smgrnblocks(relation->rd_smgr, fork_num);
    }
}

/*
 * PartitionGetNumberOfBlocksInFork
 *		Determines the current number of pages in the partition.
 */
BlockNumber PartitionGetNumberOfBlocksInFork(Relation relation, Partition partition, ForkNumber fork_num, bool estimate)
{
    BlockNumber result = 0;

    // Just return the relpages in pg_class for column-store relation.
    // Future: new interface should be implemented to calculate the number of blocks.
    if (RelationIsColStore(relation) || RelationIsTsStore(relation)) {
        return (BlockNumber)relation->rd_rel->relpages;
    }

    if (RELATION_OWN_BUCKETKEY(relation)) {
        Relation buckRel = NULL;

        oidvector *bucketlist = searchHashBucketByOid(relation->rd_bucketoid);

        if (estimate) {
            buckRel = bucketGetRelation(relation, partition, bucketlist->values[0]);
            result += smgrnblocks(buckRel->rd_smgr, fork_num) * bucketlist->dim1;
            bucketCloseRelation(buckRel);

            /*
             * If the result is suspiciously small,
             * we take the relpages from partition data instead.
             */
            if (result < ESTIMATED_MIN_BLOCKS) {
                result = partition->pd_part->relpages;
            }
        } else {
            for (int i = 0; i < bucketlist->dim1; i++) {
                buckRel = bucketGetRelation(relation, partition, bucketlist->values[i]);
                result += smgrnblocks(buckRel->rd_smgr, fork_num);
                bucketCloseRelation(buckRel);
            }
        }
    } else {
        /* Open it at the smgr level if not already done */
        PartitionOpenSmgr(partition);
        result = smgrnblocks(partition->pd_smgr, fork_num);
    }
    return result;
}

/*
 * BufferIsPermanent
 *		Determines whether a buffer will potentially still be around after
 *		a crash.  Caller must hold a buffer pin.
 */
bool BufferIsPermanent(Buffer buffer)
{
    volatile BufferDesc *buf_desc = NULL;

    /* Local buffers are used only for temp relations. */
    if (BufferIsLocal(buffer)) {
        return false;
    }

    /* Make sure we've got a real buffer, and that we hold a pin on it. */
    Assert(BufferIsValid(buffer));
    Assert(BufferIsPinned(buffer));

    /*
     * BM_PERMANENT can't be changed while we hold a pin on the buffer, so we
     * need not bother with the buffer header spinlock.  Even if someone else
     * changes the buffer header flags while we're doing this, we assume that
     * changing an aligned 2-byte BufFlags value is atomic, so we'll read the
     * old value or the new value, but not random garbage.
     */
    buf_desc = GetBufferDescriptor(buffer - 1);
    return (pg_atomic_read_u32(&buf_desc->state) & BM_PERMANENT) != 0;
}

/*
 * Retrieves the LSN of the buffer atomically using a buffer header lock.
 * This is necessary for some callers who may not have an exclusive lock on the buffer.
 */
XLogRecPtr BufferGetLSNAtomic(Buffer buffer)
{
    BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
    char *page = BufferGetPage(buffer);
    XLogRecPtr lsn;

    uint32 buf_state;
    /* If we don't need locking for correctness, fastpath out. */
    if (BufferIsLocal(buffer)) {
        return PageGetLSN(page);
    }

    /* Make sure we've got a real buffer, and that we hold a pin on it. */
    Assert(BufferIsValid(buffer));
    Assert(BufferIsPinned(buffer));

    buf_state = LockBufHdr(buf_desc);
    lsn = PageGetLSN(page);
    UnlockBufHdr(buf_desc, buf_state);

    return lsn;
}

void DropSegRelNodeSharedBuffer(RelFileNode node, ForkNumber forkNum)
{
    for (int i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;

        if (buf_desc->seg_fileno != node.relNode || buf_desc->tag.rnode.spcNode != node.spcNode ||
            buf_desc->tag.rnode.dbNode != node.dbNode) {
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (buf_desc->seg_fileno == node.relNode && buf_desc->tag.rnode.spcNode == node.spcNode &&
            buf_desc->tag.rnode.dbNode == node.dbNode && buf_desc->tag.forkNum == forkNum) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }

    for (int i = SegmentBufferStartID; i < TOTAL_BUFFER_NUM; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        if (buf_desc->tag.rnode.spcNode != node.spcNode || buf_desc->tag.rnode.dbNode != node.dbNode) {
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (buf_desc->tag.rnode.spcNode == node.spcNode && buf_desc->tag.rnode.dbNode == node.dbNode &&
            buf_desc->tag.rnode.relNode == node.relNode && buf_desc->tag.forkNum == forkNum) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
}

/* RangeForgetBuffer
 *
 */
void RangeForgetBuffer(RelFileNode node, ForkNumber forkNum, BlockNumber firstDelBlock,
    BlockNumber endDelBlock)
{
    for (int i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;

        if (!RelFileNodeEquals(buf_desc->tag.rnode, node))
            continue;

        buf_state = LockBufHdr(buf_desc);
        if (RelFileNodeEquals(buf_desc->tag.rnode, node) && buf_desc->tag.forkNum == forkNum &&
            buf_desc->tag.blockNum >= firstDelBlock && buf_desc->tag.blockNum < endDelBlock)
            InvalidateBuffer(buf_desc); /* releases spinlock */
        else
            UnlockBufHdr(buf_desc, buf_state);
    }
}

void DropRelFileNodeShareBuffers(RelFileNode node, ForkNumber forkNum, BlockNumber firstDelBlock)
{
    int i;

    for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        /*
         * We can make this a tad faster by prechecking the buffer tag before
         * we attempt to lock the buffer; this saves a lot of lock
         * acquisitions in typical cases.  It should be safe because the
         * caller must have AccessExclusiveLock on the relation, or some other
         * reason to be certain that no one is loading new pages of the rel
         * into the buffer pool.  (Otherwise we might well miss such pages
         * entirely.)  Therefore, while the tag might be changing while we
         * look at it, it can't be changing *to* a value we care about, only
         * *away* from such a value.  So false negatives are impossible, and
         * false positives are safe because we'll recheck after getting the
         * buffer lock.
         *
         * We could check forkNum and blockNum as well as the rnode, but the
         * incremental win from doing so seems small.
         */
        if (!RelFileNodeEquals(buf_desc->tag.rnode, node))
            continue;

        buf_state = LockBufHdr(buf_desc);
        if (RelFileNodeEquals(buf_desc->tag.rnode, node) && buf_desc->tag.forkNum == forkNum &&
            buf_desc->tag.blockNum >= firstDelBlock)
            InvalidateBuffer(buf_desc); /* releases spinlock */
        else
            UnlockBufHdr(buf_desc, buf_state);
    }
}

/*
 * DropRelFileNodeBuffers - This function removes from the buffer pool all the
 * pages of the specified relation fork that have block numbers >= firstDelBlock.
 * (In particular, with firstDelBlock = 0, all pages are removed.)
 * Dirty pages are simply dropped, without bothering to write them
 * out first. Therefore, this is NOT rollback-able, and so should be
 * used only with extreme caution!
 *
 * Currently, this is called only from smgr.c when the underlying file
 * is about to be deleted or truncated (firstDelBlock is needed for
 * the truncation case).  The data in the affected pages would therefore
 * be deleted momentarily anyway, and there is no point in writing it.
 * It is the responsibility of higher-level code to ensure that the
 * deletion or truncation does not lose any data that could be needed
 * later. It is also the responsibility of higher-level code to ensure
 * that no other process could be trying to load more pages of the
 * relation into buffers.
 *
 * XXX currently it sequentially searches the buffer pool, should be
 * changed to more clever ways of searching.  However, this routine
 * is used only in code paths that aren't very performance-critical,
 * and we shouldn't slow down the hot paths to make it faster ...
 */
void DropRelFileNodeBuffers(const RelFileNodeBackend &rnode, ForkNumber forkNum, BlockNumber firstDelBlock)
{
    Assert(IsHeapFileNode(rnode.node));
    gstrace_entry(GS_TRC_ID_DropRelFileNodeBuffers);

    /* If it's a local relation, it's localbuf.c's problem. */
    if (RelFileNodeBackendIsTemp(rnode)) {
        if (rnode.backend == BackendIdForTempRelations) {
            DropRelFileNodeLocalBuffers(rnode.node, forkNum, firstDelBlock);
        }

        gstrace_exit(GS_TRC_ID_DropRelFileNodeBuffers);
        return;
    }

    DropRelFileNodeShareBuffers(rnode.node, forkNum, firstDelBlock);
    gstrace_exit(GS_TRC_ID_DropRelFileNodeBuffers);
}

/*
 * DropRelFileNodeAllBuffers - This function removes from the buffer pool
 * all the pages of all forks of the specified relation. It's equivalent to calling
 * DropRelFileNodeBuffers once per fork with firstDelBlock = 0.
 */
void DropTempRelFileNodeAllBuffers(const RelFileNodeBackend& rnode)
{
    gstrace_entry(GS_TRC_ID_DropRelFileNodeAllBuffers);

    /* If it's a local relation, it's localbuf.c's problem. */
    if (RelFileNodeBackendIsTemp(rnode)) {
        if (rnode.backend == BackendIdForTempRelations) {
            DropRelFileNodeAllLocalBuffers(rnode.node);
        }
    }
    gstrace_exit(GS_TRC_ID_DropRelFileNodeAllBuffers);
}


/*
 * DropRelFileNodeAllBuffers - This function removes from the buffer pool
 * all the pages of all forks of the relfilenode_hashtbl. It's equivalent to calling
 * DropRelFileNodeBuffers
 */
void DropRelFileNodeAllBuffersUsingHash(HTAB *relfilenode_hashtbl)
{
    int i;

    for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        bool found = false;
        bool equal = false;
        bool find_dir = false;

        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        RelFileNode rd_node_snapshot = buf_desc->tag.rnode;

        if (IsBucketFileNode(rd_node_snapshot)) {
            /* bucket buffer */
            RelFileNode rd_node_bucketdir = rd_node_snapshot;
            rd_node_bucketdir.bucketNode = SegmentBktId;
            hash_search(relfilenode_hashtbl, &(rd_node_bucketdir), HASH_FIND, &found);
            find_dir = found;
            if (!found) {
                hash_search(relfilenode_hashtbl, &(rd_node_snapshot), HASH_FIND, &found);
            }
        } else {
            /* no bucket buffer */
            hash_search(relfilenode_hashtbl, &rd_node_snapshot, HASH_FIND, &found);
        }
        if (!found) {
            continue;
        }
        buf_state = LockBufHdr(buf_desc);
        if (find_dir) {
            // matching the bucket dir
            equal = RelFileNodeRelEquals(buf_desc->tag.rnode, rd_node_snapshot);
        } else {
            equal = RelFileNodeEquals(buf_desc->tag.rnode, rd_node_snapshot);
        }
        
        if (equal == true) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
}

/*
 * drop_rel_one_fork_buffers_use_hash - This function removes from the buffer pool
 * all the pages of one fork of the relfilenode_hashtbl.
 */
void DropRelFileNodeOneForkAllBuffersUsingHash(HTAB *relfilenode_hashtbl)
{
    int i;
    for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        bool found = false;
        bool equal = false;
        bool find_dir = false;
        ForkRelFileNode rd_node_snapshot;

        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        rd_node_snapshot.rnode = buf_desc->tag.rnode;
        rd_node_snapshot.forkNum = buf_desc->tag.forkNum;

        if (IsBucketFileNode(rd_node_snapshot.rnode)) {
            /* bucket buffer */
            ForkRelFileNode rd_node_bucketdir = rd_node_snapshot;
            rd_node_bucketdir.rnode.bucketNode = SegmentBktId;
            hash_search(relfilenode_hashtbl, &(rd_node_bucketdir), HASH_FIND, &found);
            find_dir = found;

            if (!find_dir) {
                hash_search(relfilenode_hashtbl, &(rd_node_snapshot), HASH_FIND, &found);
            }
        } else {
            /* no bucket buffer */
            hash_search(relfilenode_hashtbl, &rd_node_snapshot, HASH_FIND, &found);
        }

        if (!found) {
            continue;
        }

        buf_state = LockBufHdr(buf_desc);

        if (find_dir) {
            // matching the bucket dir
            equal = RelFileNodeRelEquals(buf_desc->tag.rnode, rd_node_snapshot.rnode) &&
                buf_desc->tag.forkNum == rd_node_snapshot.forkNum;
        } else {
            equal = RelFileNodeEquals(buf_desc->tag.rnode, rd_node_snapshot.rnode) &&
                buf_desc->tag.forkNum == rd_node_snapshot.forkNum;
        }

        if (equal == true) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
}

static FORCE_INLINE int compare_rnode_func(const void *left, const void *right)
{
    int128 res = (int128)((*(const uint128 *)left) - (*(const uint128 *)right));
    if (res > 0) {
        return 1;
    } else if (res < 0) {
        return -1;
    } else {
        return 0;
    }
}

static FORCE_INLINE int CheckRnodeMatchResult(const RelFileNode *rnode, int rnode_len, const RelFileNode *tag_node)
{
    Assert(rnode_len > 0);
    if (rnode_len == 1) {
        return (RelFileNodeEquals(rnode[0], *tag_node) ? 0 : -1);
    }

    int low = 0;
    int high = rnode_len - 1;
    int medium, res;
    while (low <= high) {
        medium = (low + high) / 2;
        res = compare_rnode_func(rnode + medium, tag_node);
        if (res > 0) {
            high = medium - 1;
        } else if (res < 0) {
            low = medium + 1;
        } else {
            return medium;
        }
    }
    return -1;
}

static FORCE_INLINE void ScanCompareAndInvalidateBuffer(const RelFileNode *rnodes, int rnode_len, int buffer_idx)
{
    int match_idx = -1;
    bool equal = false;
    bool find_dir = false;

    BufferDesc *bufHdr = GetBufferDescriptor(buffer_idx);
    RelFileNode tag_rnode = bufHdr->tag.rnode;
    RelFileNode tag_rnode_bktdir = tag_rnode;

    if (IsBucketFileNode(tag_rnode)) {
        tag_rnode_bktdir.bucketNode = SegmentBktId;
        match_idx = CheckRnodeMatchResult(rnodes, rnode_len, &tag_rnode_bktdir);
        find_dir = (match_idx != -1);
    }

    if (!find_dir) {
        match_idx = CheckRnodeMatchResult(rnodes, rnode_len, &tag_rnode);
    }
    
    if (SECUREC_LIKELY(-1 == match_idx)) {
        return;
    }

    uint32 buf_state = LockBufHdr(bufHdr);

    if (find_dir) {
        equal = RelFileNodeRelEquals(bufHdr->tag.rnode, rnodes[match_idx]);
    } else {
        equal = RelFileNodeEquals(bufHdr->tag.rnode, rnodes[match_idx]);
    }

    if (equal == true) {
        InvalidateBuffer(bufHdr); /* releases spinlock */
    } else {
        UnlockBufHdr(bufHdr, buf_state);
    }
}

void DropRelFileNodeAllBuffersUsingScan(RelFileNode *rnodes, int rnode_len)
{
    if (SECUREC_LIKELY(rnode_len <= 0)) {
        return;
    }

    pg_qsort(rnodes, (size_t)rnode_len, sizeof(RelFileNode), compare_rnode_func);

    Assert((g_instance.attr.attr_storage.NBuffers % 4) == 0);
    int i;
    for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i += 4) {
        ScanCompareAndInvalidateBuffer(rnodes, rnode_len, i);

        ScanCompareAndInvalidateBuffer(rnodes, rnode_len, i + 1);

        ScanCompareAndInvalidateBuffer(rnodes, rnode_len, i + 2);

        ScanCompareAndInvalidateBuffer(rnodes, rnode_len, i + 3);
    }

    gstrace_exit(GS_TRC_ID_DropRelFileNodeAllBuffers);
}

/* ---------------------------------------------------------------------
 *		DropDatabaseBuffers
 *
 *		This function removes all the buffers in the buffer cache for a
 *		particular database.  Dirty pages are simply dropped, without
 *		bothering to write them out first.	This is used when we destroy a
 *		database, to avoid trying to flush data to disk when the directory
 *		tree no longer exists.	Implementation is pretty similar to
 *		DropRelFileNodeBuffers() which is for destroying just one relation.
 * --------------------------------------------------------------------
 */
void DropDatabaseBuffers(Oid dbid)
{
    int i;

    /*
     * We needn't consider local buffers, since by assumption the target
     * database isn't our own.
     */
    gstrace_entry(GS_TRC_ID_DropDatabaseBuffers);
    
    for (i = 0; i < TOTAL_BUFFER_NUM; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        if (buf_desc->tag.rnode.dbNode != dbid) {
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (buf_desc->tag.rnode.dbNode == dbid) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
    gstrace_exit(GS_TRC_ID_DropDatabaseBuffers);
}

/* -----------------------------------------------------------------
 *		PrintBufferDescs
 *
 *		this function prints all the buffer descriptors, for debugging
 *		use only.
 * -----------------------------------------------------------------
 */
#ifdef NOT_USED
void PrintBufferDescs(void)
{
    int i;
    volatile BufferDesc *buf = t_thrd.storage_cxt.BufferDescriptors;

    for (i = 0; i < TOTAL_BUFFER_NUM; ++i, ++buf) {
        /* theoretically we should lock the bufhdr here */
        ereport(LOG, (errmsg("[%02d] (rel=%s, "
                             "blockNum=%u, flags=0x%x, refcount=%u %d)",
                             i, relpathbackend(buf->tag.rnode, InvalidBackendId, buf->tag.forkNum), buf->tag.blockNum,
                             buf->flags, buf->refcount, GetPrivateRefCount(i + 1))));
    }
}
#endif

#ifdef NOT_USED
void PrintPinnedBufs(void)
{
    int i;
    volatile BufferDesc *buf = t_thrd.storage_cxt.BufferDescriptors;

    for (i = 0; i < TOTAL_BUFFER_NUM; ++i, ++buf) {
        if (GetPrivateRefCount(i + 1) > 0) {
            /* theoretically we should lock the bufhdr here */
            ereport(LOG, (errmsg("[%02d] (rel=%s, "
                                 "blockNum=%u, flags=0x%x, refcount=%u %d)",
                                 i, relpath(buf->tag.rnode, buf->tag.forkNum), buf->tag.blockNum, buf->flags,
                                 buf->refcount, GetPrivateRefCount(i + 1))));
        }
    }
}
#endif

static inline bool flush_buffer_match(BufferDesc *buf_desc, Relation rel, Oid db_id)
{
    if (rel != NULL) {
        return RelFileNodeRelEquals(buf_desc->tag.rnode, rel->rd_node);
    }
    return (buf_desc->tag.rnode.dbNode == db_id);
}

/**
 * wait page_writer to flush it for us. Since relation lock or database lock is hold,
 * no new modification to the relation and database dirty the current pages.
 */
static void flush_wait_page_writer(BufferDesc *buf_desc, Relation rel, Oid db_id)
{
    uint32 buf_state;
    for (;;) {
        buf_state = LockBufHdr(buf_desc);
        if (flush_buffer_match(buf_desc, rel, db_id) && dw_buf_valid_dirty(buf_state)) {
            UnlockBufHdr(buf_desc, buf_state);
            pg_usleep(MILLISECOND_TO_MICROSECOND);
        } else {
            UnlockBufHdr(buf_desc, buf_state);
            break;
        }
    }
}

/**
 * rel NULL for db flush, not NULL for relation flush
 */
void flush_all_buffers(Relation rel, Oid db_id, HTAB *hashtbl)
{
    int i;
    BufferDesc *buf_desc = NULL;
    uint32 buf_state;
    uint32 size = 0;
    uint32 total = 0;

    // @Temp Table. no relation use local buffer. Temp table now use shared buffer.
    /* Make sure we can handle the pin inside the loop */
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    for (i = 0; i < NORMAL_SHARED_BUFFER_NUM; i++) {
        buf_desc = GetBufferDescriptor(i);
        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        if (!flush_buffer_match(buf_desc, rel, db_id)) {
            continue;
        }
        /* If page_writer is enabled, we just wait for the page_writer to flush the buffer. */
        if (dw_page_writer_running()) {
            flush_wait_page_writer(buf_desc, rel, db_id);
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (!flush_buffer_match(buf_desc, rel, db_id) || !dw_buf_valid_dirty(buf_state)) {
            UnlockBufHdr(buf_desc, buf_state);
            continue;
        }

        PinBuffer_Locked(buf_desc);
        (void)LWLockAcquire(buf_desc->content_lock, LW_SHARED);

        if (rel != NULL && !IsBucketFileNode(buf_desc->tag.rnode)) {
            FlushBuffer(buf_desc, rel->rd_smgr);
        } else {
            if (hashtbl != NULL) {
                (void)hash_search(hashtbl, &(buf_desc->tag.rnode.bucketNode), HASH_ENTER, NULL);
            }
            FlushBuffer(buf_desc, NULL);
        }

        LWLockRelease(buf_desc->content_lock);
        UnpinBuffer(buf_desc, true);
    }
    ereport(DW_LOG_LEVEL,
            (errmsg("double write flush %s size %u, total %u", ((rel == NULL) ? "db" : "rel"), size, total)));
}

/* ---------------------------------------------------------------------
 *		FlushRelationBuffers
 *
 *		This function writes all dirty pages of a relation out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the relation.
 *
 *		Generally, the caller should be holding AccessExclusiveLock on the
 *		target relation to ensure that no other backend is busy dirtying
 *		more blocks of the relation; the effects can't be expected to last
 *		after the lock is released.
 *
 *		XXX currently it sequentially searches the buffer pool, should be
 *		changed to more clever ways of searching.  This routine is not
 *		used in any performance-critical code paths, so it's not worth
 *		adding additional overhead to normal paths to make it go faster;
 *		but see also DropRelFileNodeBuffers.
 * --------------------------------------------------------------------
 */
void FlushRelationBuffers(Relation rel, HTAB *hashtbl)
{
    /* Open rel at the smgr level if not already done */
    gstrace_entry(GS_TRC_ID_FlushRelationBuffers);
    RelationOpenSmgr(rel);

    flush_all_buffers(rel, InvalidOid, hashtbl);
    gstrace_exit(GS_TRC_ID_FlushRelationBuffers);
}

/* ---------------------------------------------------------------------
 *		FlushDatabaseBuffers
 *
 *		This function writes all dirty pages of a database out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the database.
 *
 *		Generally, the caller should be holding an appropriate lock to ensure
 *		no other backend is active in the target database; otherwise more
 *		pages could get dirtied.
 *
 *		Note we don't worry about flushing any pages of temporary relations.
 *		It's assumed these wouldn't be interesting.
 * --------------------------------------------------------------------
 */
void FlushDatabaseBuffers(Oid dbid)
{
    gstrace_entry(GS_TRC_ID_FlushDatabaseBuffers);
    flush_all_buffers(NULL, dbid);
    gstrace_exit(GS_TRC_ID_FlushDatabaseBuffers);
}

/*
 * ReleaseBuffer -- release the pin on a buffer
 */
void ReleaseBuffer(Buffer buffer)
{
    BufferDesc *buf_desc = NULL;
    PrivateRefCountEntry *ref = NULL;

    if (!BufferIsValid(buffer)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("bad buffer ID: %d", buffer))));
    }

    ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, buffer);

    if (BufferIsLocal(buffer)) {
        Assert(u_sess->storage_cxt.LocalRefCount[-buffer - 1] > 0);
        u_sess->storage_cxt.LocalRefCount[-buffer - 1]--;
        return;
    }

    buf_desc = GetBufferDescriptor(buffer - 1);

    PrivateRefCountEntry *free_entry = NULL;
    ref = GetPrivateRefCountEntryFast(buffer, free_entry);
    if (ref == NULL) {
        ref = GetPrivateRefCountEntrySlow(buffer, false, false, free_entry);
    }
    Assert(ref != NULL);
    Assert(ref->refcount > 0);

    if (ref->refcount > 1) {
        ref->refcount--;
    } else {
        UnpinBuffer(buf_desc, false);
    }
}

/*
 * UnlockReleaseBuffer -- release the content lock and pin on a buffer
 *
 * This is just a shorthand for a common combination.
 */
FORCE_INLINE
void UnlockReleaseBuffer(Buffer buffer)
{
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
}

/*
 * IncrBufferRefCount
 *		Increment the pin count on a buffer that we have *already* pinned
 *		at least once.
 *
 *		This function cannot be used on a buffer we do not have pinned,
 *		because it doesn't change the shared buffer state.
 */
void IncrBufferRefCount(Buffer buffer)
{
    Assert(BufferIsPinned(buffer));
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    if (BufferIsLocal(buffer)) {
        u_sess->storage_cxt.LocalRefCount[-buffer - 1]++;
    } else {
        PrivateRefCountEntry* ref = NULL;
        PrivateRefCountEntry *free_entry = NULL;
        ref = GetPrivateRefCountEntryFast(buffer, free_entry);
        if (ref == NULL) {
            ref = GetPrivateRefCountEntrySlow(buffer, false, true, free_entry);
        }
        Assert(ref != NULL);
        ref->refcount++;
    }
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, buffer);
}

/*
 * MarkBufferDirtyHint
 *
 *	Mark a buffer dirty for non-critical changes.
 *
 * This is essentially the same as MarkBufferDirty, except:
 *
 * 1. The caller does not write WAL; so if checksums are enabled, we may need
 *	  to write an XLOG_FPI WAL record to protect against torn pages.
 * 2. The caller might have only share-lock instead of exclusive-lock on the
 *	  buffer's content lock.
 * 3. This function does not guarantee that the buffer is always marked dirty
 *	  (due to a race condition), so it cannot be used for important changes.
 */
void MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
{
    BufferDesc *buf_desc = NULL;
    Page page = BufferGetPage(buffer);

    if (!BufferIsValid(buffer)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("bad buffer ID: %d", buffer))));
    }

    if (BufferIsLocal(buffer)) {
        MarkLocalBufferDirty(buffer);
        return;
    }

    buf_desc = GetBufferDescriptor(buffer - 1);

    Assert(GetPrivateRefCount(buffer) > 0);
    /* here, either share or exclusive lock is OK */
    if (!LWLockHeldByMe(buf_desc->content_lock))
        ereport(PANIC, (errcode(ERRCODE_INVALID_BUFFER),
                        (errmsg("current lock of buffer %d is not held by the current thread", buffer))));

    /*
     * This routine might get called many times on the same page, if we are
     * making the first scan after commit of an xact that added/deleted many
     * tuples. So, be as quick as we can if the buffer is already dirty.  We
     * do this by not acquiring spinlock if it looks like the status bits are
     * already set.  Since we make this test unlocked, there's a chance we
     * might fail to notice that the flags have just been cleared, and failed
     * to reset them, due to memory-ordering issues.  But since this function
     * is only intended to be used in cases where failing to write out the
     * data would be harmless anyway, it doesn't really matter.
     */
    if ((pg_atomic_read_u32(&buf_desc->state) & (BM_DIRTY | BM_JUST_DIRTIED)) != (BM_DIRTY | BM_JUST_DIRTIED)) {
        XLogRecPtr lsn = InvalidXLogRecPtr;
        bool delayChkpt = false;
        uint32 buf_state;
        uint32 old_buf_state;

        /*
         * If we need to protect hint bit updates from torn writes, WAL-log a
         * full page image of the page. This full page image is only necessary
         * if the hint bit update is the first change to the page since the
         * last checkpoint.
         *
         * We don't check full_page_writes here because that logic is included
         * when we call XLogInsert() since the value changes dynamically.
         * The incremental checkpoint is protected by the doublewriter, the
         * half-write problem does not occur.
         */
        if (!ENABLE_INCRE_CKPT && XLogHintBitIsNeeded() &&
            (pg_atomic_read_u32(&buf_desc->state) & BM_PERMANENT)) {
            /*
             * If we're in recovery we cannot dirty a page because of a hint.
             * We can set the hint, just not dirty the page as a result so the
             * hint is lost when we evict the page or shutdown.
             *
             * See src/backend/storage/page/README for longer discussion.
             */
            if (RecoveryInProgress()) {
                return;
            }

            /*
             * If the block is already dirty because we either made a change
             * or set a hint already, then we don't need to write a full page
             * image.  Note that aggressive cleaning of blocks dirtied by hint
             * bit setting would increase the call rate. Bulk setting of hint
             * bits would reduce the call rate...
             *
             * We must issue the WAL record before we mark the buffer dirty.
             * Otherwise we might write the page before we write the WAL. That
             * causes a race condition, since a checkpoint might occur between
             * writing the WAL record and marking the buffer dirty. We solve
             * that with a kluge, but one that is already in use during
             * transaction commit to prevent race conditions. Basically, we
             * simply prevent the checkpoint WAL record from being written
             * until we have marked the buffer dirty. We don't start the
             * checkpoint flush until we have marked dirty, so our checkpoint
             * must flush the change to disk successfully or the checkpoint
             * never gets written, so crash recovery will fix.
             *
             * It's possible we may enter here without an xid, so it is
             * essential that CreateCheckpoint waits for virtual transactions
             * rather than full transactionids.
             */
            t_thrd.pgxact->delayChkpt = delayChkpt = true;
            lsn = XLogSaveBufferForHint(buffer, buffer_std);
            bool needSetFlag = !PageIsNew(page) && !XLogRecPtrIsInvalid(lsn);
            if (needSetFlag) {
                PageSetJustAfterFullPageWrite(page);
            }
        }

        old_buf_state = LockBufHdr(buf_desc);

        Assert(BUF_STATE_GET_REFCOUNT(old_buf_state) > 0);

        if (!(old_buf_state & BM_DIRTY)) {
            /*
             * Set the page LSN if we wrote a backup block. We aren't supposed
             * to set this when only holding a share lock but as long as we
             * serialise it somehow we're OK. We choose to set LSN while
             * holding the buffer header lock, which causes any reader of an
             * LSN who holds only a share lock to also obtain a buffer header
             * lock before using PageGetLSN(), which is enforced in BufferGetLSNAtomic().
             *
             * If checksums are enabled, you might think we should reset the
             * checksum here. That will happen when the page is written
             * sometime later in this checkpoint cycle.
             */
            if (!XLogRecPtrIsInvalid(lsn) && !XLByteLT(lsn, PageGetLSN(page))) {
                PageSetLSN(page, lsn);
            }

            /* Do vacuum cost accounting */
            t_thrd.vacuum_cxt.VacuumPageDirty++;
            u_sess->instr_cxt.pg_buffer_usage->shared_blks_dirtied++;
            if (t_thrd.vacuum_cxt.VacuumCostActive) {
                t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageDirty;
            }
        }

        buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);

        /*
         * When the page is marked dirty for the first time, needs to push the dirty page queue.
         * Check the BufferDesc rec_lsn to determine whether the dirty page is in the dirty page queue.
         * If the rec_lsn is valid, dirty page is already in the queue, don't need to push it again.
         */
        if (ENABLE_INCRE_CKPT) {
            for (;;) {
                buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);
                if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf_desc->rec_lsn))) {
                    break;
                }

                if (!is_dirty_page_queue_full(buf_desc) && push_pending_flush_queue(buffer)) {
                    break;
                }
                UnlockBufHdr(buf_desc, old_buf_state);
                pg_usleep(TEN_MICROSECOND);
                old_buf_state = LockBufHdr(buf_desc);
            }
        }

        UnlockBufHdr(buf_desc, buf_state);

        if (delayChkpt)
            t_thrd.pgxact->delayChkpt = false;
    }
#ifdef USE_ASSERT_CHECKING
    recheck_page_content(buf_desc);
#endif
}

/*
 * Release buffer content locks for shared buffers.
 *
 * Used to clean up after errors.
 *
 * Currently, we can expect that lwlock.c's LWLockReleaseAll() took care
 * of releasing buffer content locks per se; the only thing we need to deal
 * with here is clearing any PIN_COUNT request that was in progress.
 */
void UnlockBuffers(void)
{
    BufferDesc *buf = t_thrd.storage_cxt.PinCountWaitBuf;

    if (buf != NULL) {
        uint32 buf_state;
        buf_state = LockBufHdr(buf);
        /*
         * Don't complain if flag bit not set; it could have been reset but we
         * got a cancel/die interrupt before getting the signal.
         */
        if ((buf_state & BM_PIN_COUNT_WAITER) != 0 && buf->wait_backend_pid == t_thrd.proc_cxt.MyProcPid) {
            buf_state &= ~BM_PIN_COUNT_WAITER;
        }

        UnlockBufHdr(buf, buf_state);

        t_thrd.storage_cxt.PinCountWaitBuf = NULL;
    }
}

void update_wait_lockid(LWLock *lock)
{
    Buffer queue_head_buffer = get_dirty_page_queue_head_buffer();
    if (!BufferIsInvalid(queue_head_buffer)) {
        BufferDesc *queue_head_buffer_desc = GetBufferDescriptor(queue_head_buffer - 1);
        if (LWLockHeldByMeInMode(queue_head_buffer_desc->content_lock, LW_EXCLUSIVE)) {
            g_instance.ckpt_cxt_ctl->backend_wait_lock = lock;
        }
    }
}

/*
 * Acquire or release the content_lock for the buffer.
 */
void LockBuffer(Buffer buffer, int mode)
{
    volatile BufferDesc *buf = NULL;
    bool need_update_lockid = false;

    Assert(BufferIsValid(buffer));
    if (BufferIsLocal(buffer)) {
        return; /* local buffers need no lock */
    }

    buf = GetBufferDescriptor(buffer - 1);

    if (dw_enabled() && t_thrd.storage_cxt.num_held_lwlocks > 0) {
        need_update_lockid = true;
    }
    if (mode == BUFFER_LOCK_UNLOCK) {
        LWLockRelease(buf->content_lock);
    } else if (mode == BUFFER_LOCK_SHARE) {
        (void)LWLockAcquire(buf->content_lock, LW_SHARED, need_update_lockid);
    } else if (mode == BUFFER_LOCK_EXCLUSIVE) {
        (void)LWLockAcquire(buf->content_lock, LW_EXCLUSIVE, need_update_lockid);
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("unrecognized buffer lock mode: %d", mode))));
    }
}

/*
 * Try to acquire the content_lock for the buffer if must_wait is false.
 * If the content lock is not available, return FALSE with no side-effects.
 */
bool TryLockBuffer(Buffer buffer, int mode, bool must_wait)
{
    Assert(BufferIsValid(buffer));

    /* without tries, act as LockBuffer */
    if (must_wait) {
        LockBuffer(buffer, mode);
        return true;
    }

    /* local buffers need no lock */
    if (BufferIsLocal(buffer)) {
        return true;
    }

    volatile BufferDesc *buf = GetBufferDescriptor(buffer - 1);

    if (mode == BUFFER_LOCK_SHARE) {
        return LWLockConditionalAcquire(buf->content_lock, LW_SHARED);
    } else if (mode == BUFFER_LOCK_EXCLUSIVE) {
        return LWLockConditionalAcquire(buf->content_lock, LW_EXCLUSIVE);
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            (errmsg("unrecognized buffer lock mode for TryLockBuffer: %d", mode))));
    }

    return false;
}

/*
 * Acquire the content_lock for the buffer, but only if we don't have to wait.
 *
 * This assumes the caller wants BUFFER_LOCK_EXCLUSIVE mode.
 */
bool ConditionalLockBuffer(Buffer buffer)
{
    volatile BufferDesc *buf = NULL;

    Assert(BufferIsValid(buffer));
    if (BufferIsLocal(buffer)) {
        return true; /* act as though we got it */
    }

    buf = GetBufferDescriptor(buffer - 1);

    return LWLockConditionalAcquire(buf->content_lock, LW_EXCLUSIVE);
}

/*
 * LockBufferForCleanup - lock a buffer in preparation for deleting items
 *
 * Items may be deleted from a disk page only when the caller (a) holds an
 * exclusive lock on the buffer and (b) has observed that no other backend
 * holds a pin on the buffer.  If there is a pin, then the other backend
 * might have a pointer into the buffer (for example, a heapscan reference
 * to an item --- see README for more details).  It's OK if a pin is added
 * after the cleanup starts, however; the newly-arrived backend will be
 * unable to look at the page until we release the exclusive lock.
 *
 * To implement this protocol, a would-be deleter must pin the buffer and
 * then call LockBufferForCleanup().  LockBufferForCleanup() is similar to
 * LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE), except that it loops until
 * it has successfully observed pin count = 1.
 */
void LockBufferForCleanup(Buffer buffer)
{
    BufferDesc *buf_desc = NULL;

    Assert(BufferIsValid(buffer));
    Assert(t_thrd.storage_cxt.PinCountWaitBuf == NULL);

    if (BufferIsLocal(buffer)) {
        /* There should be exactly one pin */
        if (u_sess->storage_cxt.LocalRefCount[-buffer - 1] != 1) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER),
                            (errmsg("incorrect local pin count: %d", u_sess->storage_cxt.LocalRefCount[-buffer - 1]))));
        }
        /* Nobody else to wait for */
        return;
    }

    /* There should be exactly one local pin */
    if (GetPrivateRefCount(buffer) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER),
                        (errmsg("incorrect local pin count: %d", GetPrivateRefCount(buffer)))));
    }
    buf_desc = GetBufferDescriptor(buffer - 1);

    for (;;) {
        uint32 buf_state;

        /* Try to acquire lock */
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        buf_state = LockBufHdr(buf_desc);

        Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1) {
            /* Successfully acquired exclusive lock with pincount 1 */
            UnlockBufHdr(buf_desc, buf_state);
            return;
        }
        /* Failed, so mark myself as waiting for pincount 1 */
        if (buf_state & BM_PIN_COUNT_WAITER) {
            UnlockBufHdr(buf_desc, buf_state);
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BUFFER), (errmsg("multiple backends attempting to wait for pincount 1"))));
        }
        buf_desc->wait_backend_pid = t_thrd.proc_cxt.MyProcPid;
        t_thrd.storage_cxt.PinCountWaitBuf = buf_desc;
        buf_state |= BM_PIN_COUNT_WAITER;
        UnlockBufHdr(buf_desc, buf_state);
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        /* Wait to be signaled by UnpinBuffer() */
        if (InHotStandby && g_supportHotStandby) {
            /* Publish the bufid that Startup process waits on */
            parallel_recovery::SetStartupBufferPinWaitBufId(buffer - 1);
            /* Set alarm and then wait to be signaled by UnpinBuffer() */
            ResolveRecoveryConflictWithBufferPin();
            /* Reset the published bufid */
            parallel_recovery::SetStartupBufferPinWaitBufId(-1);

        } else {
            ProcWaitForSignal();
        }

        /*
         * Remove flag marking us as waiter. Normally this will not be set
         * anymore, but ProcWaitForSignal() can return for other signals as
         * well.  We take care to only reset the flag if we're the waiter, as
         * theoretically another backend could have started waiting. That's
         * impossible with the current usages due to table level locking, but
         * better be safe.
         */
        buf_state = LockBufHdr(buf_desc);
        if ((buf_state & BM_PIN_COUNT_WAITER) != 0 &&
            buf_desc->wait_backend_pid == t_thrd.proc_cxt.MyProcPid) {
            buf_state &= ~BM_PIN_COUNT_WAITER;
        }
        UnlockBufHdr(buf_desc, buf_state);

        t_thrd.storage_cxt.PinCountWaitBuf = NULL;
        /* Loop back and try again */
    }
}

/*
 * Check called from RecoveryConflictInterrupt handler when Startup
 * process requests cancellation of all pin holders that are blocking it.
 */
bool HoldingBufferPinThatDelaysRecovery(void)
{
    uint32 bufLen = parallel_recovery::GetStartupBufferPinWaitBufLen();
    int bufids[MAX_RECOVERY_THREAD_NUM + 1];
    parallel_recovery::GetStartupBufferPinWaitBufId(bufids, bufLen);

    for (uint32 i = 0; i < bufLen; i++) {

        /*
         * If we get woken slowly then it's possible that the Startup process was
         * already woken by other backends before we got here. Also possible that
         * we get here by multiple interrupts or interrupts at inappropriate
         * times, so make sure we do nothing if the bufid is not set.
         */
        if (bufids[i] < 0) {
            continue;
        }

        if (GetPrivateRefCount(bufids[i] + 1) > 0) {
            return true;
        }
    }

    return false;
}

bool ConditionalLockUHeapBufferForCleanup(Buffer buffer)
{
    Assert(BufferIsValid(buffer));

    /* Try to acquire lock */
    if (!ConditionalLockBuffer(buffer)) {
        return false;
    }

    return true;
}

/*
 * ConditionalLockBufferForCleanup - as above, but don't wait to get the lock
 *
 * We won't loop, but just check once to see if the pin count is OK.  If
 * not, return FALSE with no lock held.
 */
bool ConditionalLockBufferForCleanup(Buffer buffer)
{
    BufferDesc *buf_desc = NULL;
    uint32 buf_state, refcount;

    Assert(BufferIsValid(buffer));

    if (BufferIsLocal(buffer)) {
        refcount = u_sess->storage_cxt.LocalRefCount[-buffer - 1];
        /* There should be exactly one pin */
        Assert(refcount > 0);
        if (refcount != 1)
            return false;
        /* Nobody else to wait for */
        return true;
    }

    /* There should be exactly one local pin */
    refcount = GetPrivateRefCount(buffer);
    Assert(refcount);
    if (refcount != 1) {
        return false;
    }

    /* Try to acquire lock */
    if (!ConditionalLockBuffer(buffer)) {
        return false;
    }

    buf_desc = GetBufferDescriptor(buffer - 1);
    buf_state = LockBufHdr(buf_desc);
    refcount = BUF_STATE_GET_REFCOUNT(buf_state);

    Assert(refcount > 0);
    if (refcount == 1) {
        /* Successfully acquired exclusive lock with pincount 1 */
        UnlockBufHdr(buf_desc, buf_state);
        return true;
    }

    /* Failed, so release the lock */
    UnlockBufHdr(buf_desc, buf_state);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    return false;
}

/*
 * IsBufferCleanupOK - as above, but we already have the lock
 *
 * Check whether it's OK to perform cleanup on a buffer we've already
 * locked.  If we observe that the pin count is 1, our exclusive lock
 * happens to be a cleanup lock, and we can proceed with anything that
 * would have been allowable had we sought a cleanup lock originally.
 */
bool IsBufferCleanupOK(Buffer buffer)
{
    BufferDesc *bufHdr;
    uint32 buf_state;

    Assert(BufferIsValid(buffer));

    if (BufferIsLocal(buffer)) {
        /* There should be exactly one pin */
        if (u_sess->storage_cxt.LocalRefCount[-buffer - 1] != 1)
            return false;
        /* Nobody else to wait for */
        return true;
    }

    /* There should be exactly one local pin */
    if (GetPrivateRefCount(buffer) != 1)
        return false;

    bufHdr = GetBufferDescriptor(buffer - 1);

    /* caller must hold exclusive lock on buffer */
    Assert(LWLockHeldByMeInMode(bufHdr->content_lock, LW_EXCLUSIVE));

    buf_state = LockBufHdr(bufHdr);

    Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
    if (BUF_STATE_GET_REFCOUNT(buf_state) == 1) {
        /* pincount is OK. */
        UnlockBufHdr(bufHdr, buf_state);
        return true;
    }

    UnlockBufHdr(bufHdr, buf_state);
    return false;
}

/*
 *	Functions for buffer I/O handling
 *
 *	Note: We assume that nested buffer I/O never occurs.
 *	i.e at most one io_in_progress lock is held per proc.
 *
 *	Also note that these are used only for shared buffers, not local ones.
 */
/*
 * WaitIO -- Block until the IO_IN_PROGRESS flag on 'buf' is cleared.
 */
static void WaitIO(BufferDesc *buf)
{
    /*
     * Changed to wait until there's no IO - Inoue 01/13/2000
     *
     * Note this is *necessary* because an error abort in the process doing
     * I/O could release the io_in_progress_lock prematurely. See
     * AbortBufferIO.
     */
    for (;;) {
        uint32 buf_state;

        /*
         * It may not be necessary to acquire the spinlock to check the flag
         * here, but since this test is essential for correctness, we'd better
         * play it safe.
         */
        buf_state = LockBufHdr(buf);
        UnlockBufHdr(buf, buf_state);

        if (!(buf_state & BM_IO_IN_PROGRESS)) {
            break;
        }
        (void)LWLockAcquire(buf->io_in_progress_lock, LW_SHARED);
        LWLockRelease(buf->io_in_progress_lock);
    }
}

/*
 * @Description: wait and check io state when used adio, whether in BM_IO_IN_PROGRESS or BM_IO_ERROR
 *  now only used for vacuum full
 * @Param[IN] buf: buffer desc
 * @See also:
 */
void CheckIOState(volatile void *buf_desc)
{
    BufferDesc *buf = (BufferDesc *)buf_desc;
    for (;;) {
        uint32 buf_state;

        /*
         * It may not be necessary to acquire the spinlock to check the flag
         * here, but since this test is essential for correctness, we'd better
         * play it safe.
         */
        buf_state = LockBufHdr(buf);
        UnlockBufHdr(buf, buf_state);
        if (buf_state & BM_IO_ERROR) {
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), (errmsg("CheckIOState, find an error in async write"))));
            break;
        }

        if (!(buf_state & BM_IO_IN_PROGRESS)) {
            break;
        }
        pg_usleep(1000L);
    }
}

/*
 * StartBufferIO: begin I/O on this buffer
 *	(Assumptions)
 *	My process is executing no IO
 *	The buffer is Pinned
 *
 * In some scenarios there are race conditions in which multiple backends
 * could attempt the same I/O operation concurrently.  If someone else
 * has already started I/O on this buffer then we will block on the
 * io_in_progress lock until he's done.
 *
 * Input operations are only attempted on buffers that are not BM_VALID,
 * and output operations only on buffers that are BM_VALID and BM_DIRTY,
 * so we can always tell if the work is already done.
 *
 * Returns TRUE if we successfully marked the buffer as I/O busy,
 * FALSE if someone else already did the work.
 */
bool StartBufferIO(BufferDesc *buf, bool for_input)
{
    uint32 buf_state;

    Assert(!t_thrd.storage_cxt.InProgressBuf);

    /* To check the InProgressBuf must be NULL. */
    if (t_thrd.storage_cxt.InProgressBuf) {
        ereport(PANIC, (errmsg("InProgressBuf not null: id %d flags %u, buf: id %d flags %u",
                               t_thrd.storage_cxt.InProgressBuf->buf_id,
                               pg_atomic_read_u32(&t_thrd.storage_cxt.InProgressBuf->state) & BUF_FLAG_MASK,
                               buf->buf_id, pg_atomic_read_u32(&buf->state) & BUF_FLAG_MASK)));
    }

    for (; ;) {
        /*
         * Grab the io_in_progress lock so that other processes can wait for
         * me to finish the I/O.
         */
        (void)LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);

        buf_state = LockBufHdr(buf);
        if (!(buf_state & BM_IO_IN_PROGRESS)) {
            break;
        }

        /*
         * The only way BM_IO_IN_PROGRESS could be set when the io_in_progress
         * lock isn't held is if the process doing the I/O is recovering from
         * an error (see AbortBufferIO).  If that's the case, we must wait for
         * him to get unwedged.
         */
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);
        WaitIO(buf);
    }

    /* Once we get here, there is definitely no I/O active on this buffer */
    if (for_input ? (buf_state & BM_VALID) : !(buf_state & BM_DIRTY)) {
        /* someone else already did the I/O */
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);
        return false;
    }

    buf_state |= BM_IO_IN_PROGRESS;
    UnlockBufHdr(buf, buf_state);

    t_thrd.storage_cxt.InProgressBuf = buf;
    t_thrd.storage_cxt.IsForInput = for_input;

    return true;
}

/*
 * TerminateBufferIO: release a buffer we were doing I/O on
 *	(Assumptions)
 *	My process is executing IO for the buffer
 *	BM_IO_IN_PROGRESS bit is set for the buffer
 *	We hold the buffer's io_in_progress lock
 *	The buffer is Pinned
 *
 * If clear_dirty is TRUE and BM_JUST_DIRTIED is not set, we clear the
 * buffer's BM_DIRTY flag.  This is appropriate when terminating a
 * successful write.  The check on BM_JUST_DIRTIED is necessary to avoid
 * marking the buffer clean if it was re-dirtied while we were writing.
 *
 * set_flag_bits gets ORed into the buffer's flags.  It must include
 * BM_IO_ERROR in a failure case.  For successful completion it could
 * be 0, or BM_VALID if we just finished reading in the page.
 *
 * For synchronous I/O TerminateBufferIO() is expected to be operating
 * on the InProgressBuf and resets it after setting the flags but before
 * releasing the io_in_progress_lock.  ADIO does not use the
 * thread InProgressBuf or forInput
 */
void TerminateBufferIO(volatile BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
    Assert(buf == t_thrd.storage_cxt.InProgressBuf);
    TerminateBufferIO_common((BufferDesc *)buf, clear_dirty, set_flag_bits);
    t_thrd.storage_cxt.InProgressBuf = NULL;
    LWLockRelease(((BufferDesc *)buf)->io_in_progress_lock);
}

/*
 * AsyncTerminateBufferIO: Release a buffer once I/O is done.
 * This routine is similar to TerminateBufferIO().  Except that it
 * operates on the given buffer and does not use or affect the InProgressBuf
 * or IsForInput globals.
 *
 * Like TerminateBufferIO() this routine is meant to
 * be called after a buffer is allocated for a block, to set the buf->flags
 * appropriately and to release the io_in_progress lock to allow other
 * threads to access the buffer.  This can occur after the buffer i/o is
 * complete or when the i/o is aborted prior to being started.
 *
 * This routine is an external buffer manager interface, so
 * its prototype specifies an opaque (void *) buffer type.  The buffer
 * must have the io_in_progress_lock held and may be pinned prior to calling
 * this routine.
 *
 * The routine acquires the buf header spinlock, and changes the buf->flags.
 * it leaves the buffer without the io_in_progress_lock held.
 */
void AsyncTerminateBufferIO(void *buffer, bool clear_dirty, uint32 set_flag_bits)
{
    BufferDesc *buf = (BufferDesc *)buffer;

    TerminateBufferIO_common(buf, clear_dirty, set_flag_bits);
    LWLockRelease(buf->io_in_progress_lock);
}

/*
 * TerminateBufferIO_common: Common code called by TerminateBufferIO() and
 * AsyncTerminateBufferIO() to set th buffer flags.
 */
static void TerminateBufferIO_common(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
    uint32 buf_state;

    buf_state = LockBufHdr(buf);

    Assert(buf_state & BM_IO_IN_PROGRESS);

    buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);

    if (clear_dirty) {
        if (ENABLE_INCRE_CKPT) {
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->rec_lsn))) {
                remove_dirty_page_from_queue(buf);
            } else {
                ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errcode(ERRCODE_INVALID_BUFFER),
                                (errmsg("buffer is dirty but not in dirty page queue in TerminateBufferIO_common"))));
            }

            /* The page is dirty again and needs to be re-inserted into the dirty page list. */
            if ((buf_state & BM_JUST_DIRTIED)) {
                buf_state &= ~BM_CHECKPOINT_NEEDED;
                if (!push_pending_flush_queue(BufferDescriptorGetBuffer(buf))) {
                    ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errcode(ERRCODE_INVALID_BUFFER),
                                    (errmsg("TerminateBufferIO_common, dirty page queue is full when trying to "
                                            "push buffer to the queue"))));
                }
            }
        }

        if (!(buf_state & BM_JUST_DIRTIED)) {
            buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);
        }
    }
    buf_state |= set_flag_bits;
    UnlockBufHdr(buf, buf_state);
}

/*
 * @Description: api function to clean up the I/O status
 * @See also:
 */
void AbortAsyncListIO(void)
{
    if (t_thrd.storage_cxt.InProgressAioType == AioUnkown) {
        return;
    }
    if (t_thrd.storage_cxt.InProgressAioType == AioRead) {
        PageListPrefetchAbort();
    } else {
        PageListBackWriteAbort();
    }
}

/*
 * AbortBufferIO: Clean up the active buffer I/O after an error.
 * All LWLocks we might have held have been released,
 * but we haven't yet released buffer pins, so the buffer is still pinned.
 */
void AbortBufferIO(void)
{
    BufferDesc *buf = (BufferDesc *)t_thrd.storage_cxt.InProgressBuf;
    bool isForInput = (bool)t_thrd.storage_cxt.IsForInput;

    if (buf != NULL) {
        /*
         * For Sync I/O
         * LWLockReleaseAll was already been called, so we're not holding
         * the buffer's io_in_progress_lock. We have to re-acquire it so that
         * we can use TerminateBufferIO. Anyone who's executing WaitIO on the
         * buffer will be in a busy spin until we succeed in doing this.
         */
        (void)LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);
        AbortBufferIO_common(buf, isForInput);
        TerminateBufferIO(buf, false, BM_IO_ERROR);
    }

    AbortSegBufferIO();
}

/*
 * AsyncAbortBufferIO: Clean up an active buffer I/O after an error.
 * InPtogressBuf and IsForInput globals are not used for async I/O.
 *
 * Like AbortBufferIO() this routine is used when a buffer I/O fails
 * and cannot or should not be started.  Except that it operates on the
 * given buffer and does not use or affect the InProgressBuf
 * or IsForInput globals.
 *
 * This routine requires that the buffer is valid and
 * and its io_in_progress_lock is held on entry.
 * The buffer must be pinned and share locked.  The routine changes the
 * buf->flags and unlocks the io_in_progress_lock.
 * It reports repeated failures.
 */
extern void AsyncAbortBufferIO(void *buffer, bool isForInput)
{
    BufferDesc *buf = (BufferDesc *)buffer;
    AbortBufferIO_common(buf, isForInput);
    AsyncTerminateBufferIO(buf, false, BM_IO_ERROR);
}

/*
 * @Description: clear and set flag, request by vacuum full in adio mode
 * @Param[IN] buffer: buffer desc
 * @See also:
 */
extern void AsyncTerminateBufferIOByVacuum(void *buffer)
{
    BufferDesc *buf = (BufferDesc *)buffer;
    TerminateBufferIO_common(buf, true, 0);
}

/*
 * @Description: clear and set flag, request by vacuum full in adio mode when error occor
 * @Param[IN] buffer: buffer desc
 * @See also:
 */
extern void AsyncAbortBufferIOByVacuum(void *buffer)
{
    BufferDesc *buf = (BufferDesc *)buffer;
    TerminateBufferIO_common(buf, true, BM_IO_ERROR);
}

/*
 *  AbortBufferIO_common: Clean up active sync/async buffer I/O after an error.
 *
 *  For a single synchronous I/O, the caller passes buf=InProgressBuf, isInput=IsForInput.
 *
 *  For async I/O the context of the I/O is only within the AIO requests so
 *  the caller passes the buf, isInput=true/false for reads/writes,
 *  and isInProgressLockHeld=true/false as applicable.
 *  For ADIO, the LW locks are held until the I/O has been tried.
 *
 *	If I/O was in progress, we always set BM_IO_ERROR, even though it's
 *	possible the error condition was not specifically related to I/O.
 */
void AbortBufferIO_common(BufferDesc *buf, bool isForInput)
{
    uint32 buf_state;

    buf_state = LockBufHdr(buf);
    Assert(buf_state & BM_IO_IN_PROGRESS);
    if (isForInput) {
        /* When reading we expect the buffer to be invalid but not dirty */
        Assert(!(buf_state & BM_DIRTY));
        Assert(!(buf_state & BM_VALID));
        UnlockBufHdr(buf, buf_state);
    } else {
        /* When writing we expect the buffer to be valid and dirty */
        Assert(buf_state & BM_DIRTY);
        buf_state &= ~BM_CHECKPOINT_NEEDED;
        UnlockBufHdr(buf, buf_state);
        /* Issue notice if this is not the first failure... */
        if (buf_state & BM_IO_ERROR) {
            /* Buffer is pinned, so we can read tag without spinlock */
            char *path = NULL;

            path = relpathperm(buf->tag.rnode, buf->tag.forkNum);
            ereport(WARNING,
                    (errcode(ERRCODE_IO_ERROR), errmsg("could not write block %u of %s", buf->tag.blockNum, path),
                     errdetail("Multiple failures --- write error might be permanent.")));
            pfree(path);
        }
    }
}

/*
 * Error context callback for errors occurring during shared buffer writes.
 */
void shared_buffer_write_error_callback(void *arg)
{
    volatile BufferDesc *buf_desc = (volatile BufferDesc *)arg;
    ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];

    if (edata->elevel >= ERROR) {
        reset_dw_pos_flag();
    }

    /* Buffer is pinned, so we can read the tag without locking the spinlock */
    if (buf_desc != NULL) {
        char *path = relpathperm(((BufferDesc *)buf_desc)->tag.rnode, ((BufferDesc *)buf_desc)->tag.forkNum);

        (void)errcontext("writing block %u of relation %s", buf_desc->tag.blockNum, path);
        pfree(path);
    }
}

/*
 * Lock buffer header - set BM_LOCKED in buffer state.
 */
uint32 LockBufHdr(BufferDesc *desc)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delayStatus = init_spin_delay(desc);
#endif
    uint32 old_buf_state;

    while (true) {
        /* set BM_LOCKED flag */
        old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
        /* if it wasn't set before we're OK */
        if (!(old_buf_state & BM_LOCKED))
            break;
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delayStatus);
#endif
    }
#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delayStatus);
#endif

    /* ENABLE_THREAD_CHECK only, acquire semantic */
    TsAnnotateHappensAfter(&desc->state);

    return old_buf_state | BM_LOCKED;
}

const int MAX_SPINS_RETRY_TIMES = 100;
bool retryLockBufHdr(BufferDesc *desc, uint32 *buf_state)
{
    uint32 old_buf_state = pg_atomic_read_u32(&desc->state);
    uint32 retry_times = 0;

    /* set BM_LOCKED flag */
    for (retry_times = 0; retry_times < MAX_SPINS_RETRY_TIMES; retry_times++) {
        old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
        /* if it wasn't set before we're OK */
        if (!(old_buf_state & BM_LOCKED)) {
            *buf_state = old_buf_state | BM_LOCKED;

            /* ENABLE_THREAD_CHECK only, acquire semantic */
            TsAnnotateHappensAfter(&desc->state);

            return true;
        }
        /* CPU-specific delay each time through the loop */
        SPIN_DELAY();
    }

    *buf_state = old_buf_state;
    return false;
}

/*
 * Wait until the BM_LOCKED flag isn't set anymore and return the buffer's
 * state at that point.
 *
 * Obviously the buffer could be locked by the time the value is returned, so
 * this is primarily useful in CAS style loops.
 */
static uint32 WaitBufHdrUnlocked(BufferDesc *buf)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delay_status = init_spin_delay(buf);
#endif
    uint32 buf_state;

    buf_state = pg_atomic_read_u32(&buf->state);

    while (buf_state & BM_LOCKED) {
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delay_status);
#endif
        buf_state = pg_atomic_read_u32(&buf->state);
    }

#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delay_status);
#endif

    /* ENABLE_THREAD_CHECK only, acqurie semantic */
    TsAnnotateHappensAfter(&buf->state);

    return buf_state;
}

/*
 * RelFileNode qsort/bsearch comparator.
 */
static int rnode_comparator(const void *p1, const void *p2)
{
    RelFileNode n1 = *(RelFileNode *)p1;
    RelFileNode n2 = *(RelFileNode *)p2;

    if (n1.bucketNode < n2.bucketNode)
        return -1;
    else if (n1.bucketNode > n2.bucketNode)
        return 1;

    if (n1.relNode < n2.relNode)
        return -1;
    else if (n1.relNode > n2.relNode)
        return 1;

    if (n1.dbNode < n2.dbNode)
        return -1;
    else if (n1.dbNode > n2.dbNode)
        return 1;

    if (n1.spcNode < n2.spcNode)
        return -1;
    else if (n1.spcNode > n2.spcNode)
        return 1;
    else
        return 0;
}

/*
 * BufferTag comparator.
 */
static int buffertag_comparator(const void *a, const void *b)
{
    const BufferTag *ba = (const BufferTag *)a;
    const BufferTag *bb = (const BufferTag *)b;
    int ret;

    ret = rnode_comparator(&ba->rnode, &bb->rnode);
    if (ret != 0) {
        return ret;
    }

    if (ba->forkNum < bb->forkNum) {
        return -1;
    }
    if (ba->forkNum > bb->forkNum) {
        return 1;
    }

    if (ba->blockNum < bb->blockNum) {
        return -1;
    }
    if (ba->blockNum > bb->blockNum) {
        return 1;
    }

    return 0;
}

/*
 * Initialize a writeback context, discarding potential previous state.
 *
 * *max_pending is a pointer instead of an immediate value, so the coalesce
 * limits can easily changed by the GUC mechanism, and so calling code does
 * not have to check the current configuration. A value is 0 means that no
 * writeback control will be performed.
 */
void WritebackContextInit(WritebackContext *context, int *max_pending)
{
    Assert(*max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

    context->max_pending = max_pending;
    context->nr_pending = 0;
}

/*
 * Add buffer to list of pending writeback requests.
 */
void ScheduleBufferTagForWriteback(WritebackContext *context, BufferTag *tag)
{
    PendingWriteback *pending = NULL;

    /*
     * Add buffer to the pending writeback array, unless writeback control is
     * disabled.
     */
    if (*context->max_pending > 0) {
        Assert(*context->max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

        pending = &context->pending_writebacks[context->nr_pending++];

        pending->tag = *tag;
    }

    /*
     * Perform pending flushes if the writeback limit is exceeded. This
     * includes the case where previously an item has been added, but control
     * is now disabled.
     */
    if (context->nr_pending >= *context->max_pending) {
        IssuePendingWritebacks(context);
    }
}

/*
 * Issue all pending writeback requests, previously scheduled with
 * ScheduleBufferTagForWriteback, to the OS.
 *
 * Because this is only used to improve the OSs IO scheduling we try to never
 * error out - it's just a hint.
 */
void IssuePendingWritebacks(WritebackContext *context)
{
    int i;

    if (context->nr_pending == 0) {
        return;
    }

    /*
     * Executing the writes in-order can make them a lot faster, and allows to
     * merge writeback requests to consecutive blocks into larger writebacks.
     */
    qsort(&context->pending_writebacks, context->nr_pending, sizeof(PendingWriteback), buffertag_comparator);

    /*
     * Coalesce neighbouring writes, but nothing else. For that we iterate
     * through the, now sorted, array of pending flushes, and look forward to
     * find all neighbouring (or identical) writes.
     */
    for (i = 0; i < context->nr_pending; i++) {
        PendingWriteback *cur = NULL;
        PendingWriteback *next = NULL;
        SMgrRelation reln;
        int ahead;
        BufferTag tag;
        Size nblocks = 1;

        cur = &context->pending_writebacks[i];
        tag = cur->tag;

        /*
         * Peek ahead, into following writeback requests, to see if they can
         * be combined with the current one.
         */
        for (ahead = 0; i + ahead + 1 < context->nr_pending; ahead++) {
            next = &context->pending_writebacks[i + ahead + 1];

            /* different file, stop */
            if (!RelFileNodeEquals(cur->tag.rnode, next->tag.rnode) || cur->tag.forkNum != next->tag.forkNum) {
                break;
            }

            /* ok, block queued twice, skip */
            if (cur->tag.blockNum == next->tag.blockNum) {
                continue;
            }

            /* only merge consecutive writes */
            if (cur->tag.blockNum + 1 != next->tag.blockNum) {
                break;
            }

            nblocks++;
            cur = next;
        }

        i += ahead;

        /*
         * And finally tell the kernel to write the data to storage, forkNum might
         * from a column relation so don't forget to get column number from forknum
         */
        reln = smgropen(tag.rnode, InvalidBackendId, GetColumnNum(tag.forkNum));
        smgrwriteback(reln, tag.forkNum, tag.blockNum, nblocks);
    }

    context->nr_pending = 0;
}

int ckpt_buforder_comparator(const void *pa, const void *pb)
{
    const CkptSortItem *a = (CkptSortItem *)pa;
    const CkptSortItem *b = (CkptSortItem *)pb;

    /* compare tablespace */
    if (a->tsId < b->tsId) {
        return -1;
    } else if (a->tsId > b->tsId) {
        return 1;
    }
    /* compare relation */
    if (a->relNode < b->relNode) {
        return -1;
    } else if (a->relNode > b->relNode) {
        return 1;
    }

    /* compare bucket */
    if (a->bucketNode < b->bucketNode) {
        return -1;
    } else if (a->bucketNode > b->bucketNode) {
        return 1;
    } else if (a->forkNum < b->forkNum) { /* compare fork */
        return -1;
    } else if (a->forkNum > b->forkNum) {
        return 1;
        /* compare block number */
    } else if (a->blockNum < b->blockNum) {
        return -1;
    } else { /* should not be the same block ... */
        return 1;
    }
    /* do not need to compare opt */
}

/*
 * Comparator for a Min-Heap over the per-tablespace checkpoint completion
 * progress.
 */
static int ts_ckpt_progress_comparator(Datum a, Datum b, void *arg)
{
    CkptTsStatus *sa = (CkptTsStatus *)a;
    CkptTsStatus *sb = (CkptTsStatus *)b;

    /* we want a min-heap, so return 1 for the a < b */
    if (sa->progress < sb->progress) {
        return 1;
    } else if (sa->progress == sb->progress) {
        return 0;
    } else {
        return -1;
    }
}


/* RemoteReadFile
 *              primary dn use this function repair file.
 */
void RemoteReadFile(RemoteReadFileKey *key, char *buf, uint32 size, int timeout, uint32* remote_size)
{
    XLogRecPtr cur_lsn = InvalidXLogRecPtr;
    int retry_times = 0;
    char *remote_address = NULL;
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */

     /* get remote address */
    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);
    remote_address = remote_address1;

    /* primary get the xlog insert loc */
    cur_lsn = GetXLogInsertRecPtr();

retry:
    if (remote_address[0] == '\0' || remote_address[0] == '@') {
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE), errmsg("remote not available")));
    }

    ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read page, file %s block start is %d from %s",
                                                relpathperm(key->relfilenode, key->forknum), key->blockstart,
                                                remote_address)));

    PROFILING_REMOTE_START();

    int ret_code = RemoteGetFile(remote_address, key, cur_lsn, size, buf, &remote_lsn, remote_size, timeout);

    PROFILING_REMOTE_END_READ(size, (ret_code == REMOTE_READ_OK));

    if (ret_code != REMOTE_READ_OK) {
        if (IS_DN_DUMMY_STANDYS_MODE() || retry_times >= 1) {
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE),
                            errmsg("remote read failed from %s, %s", remote_address, RemoteReadErrMsg(ret_code))));
        } else {
            ereport(WARNING,
                    (errmodule(MOD_REMOTE),
                     errmsg("remote read failed from %s, %s. try another", remote_address, RemoteReadErrMsg(ret_code)),
                     handle_in_client(true)));

            /* Check interrupts */
            CHECK_FOR_INTERRUPTS();

            remote_address = remote_address2;
            ++retry_times;
            goto retry; /* jump out retry_times >= 1 */
        }
    }

    return;
}

/* RemoteReadFileSize
 *              primary dn use this function get remote file size.
 */
int64 RemoteReadFileSize(RemoteReadFileKey *key, int timeout)
{
    int retry_times = 0;
    int64 size = 0;
    char *remote_address = NULL;
    XLogRecPtr cur_lsn = InvalidXLogRecPtr;
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */

     /* get remote address */
    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);
    remote_address = remote_address1;

    /* primary get the xlog insert loc */
    cur_lsn = GetXLogInsertRecPtr();

retry:
    if (remote_address[0] == '\0' || remote_address[0] == '@') {
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE), errmsg("remote not available")));
    }

    ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read page size, file %s from %s",
            relpathperm(key->relfilenode, key->forknum), remote_address)));

    PROFILING_REMOTE_START();

    int ret_code = RemoteGetFileSize(remote_address, key, cur_lsn, &size, timeout);

    PROFILING_REMOTE_END_READ(sizeof(uint64) + sizeof(uint64), (ret_code == REMOTE_READ_OK));

    if (ret_code != REMOTE_READ_OK) {
        if (IS_DN_DUMMY_STANDYS_MODE() || retry_times >= 1) {
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE),
                            errmsg("remote read failed from %s, %s", remote_address, RemoteReadErrMsg(ret_code))));
        } else {
            ereport(WARNING,
                    (errmodule(MOD_REMOTE),
                     errmsg("remote read failed from %s, %s. try another", remote_address, RemoteReadErrMsg(ret_code)),
                     handle_in_client(true)));

            /* Check interrupts */
            CHECK_FOR_INTERRUPTS();

            remote_address = remote_address2;
            ++retry_times;
            goto retry; /* jump out retry_times >= 1 */
        }
    }

    return size;
}


void RemoteReadBlock(const RelFileNodeBackend &rnode, ForkNumber fork_num, BlockNumber block_num,
                     char *buf, const XLogPhyBlock *pblk, int timeout)
{
    /* get current xlog insert lsn */
    XLogRecPtr cur_lsn = GetInsertRecPtr();

    /* get remote address */
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */
    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);

    char *remote_address = remote_address1;
    int retry_times = 0;

retry:
    if (remote_address[0] == '\0' || remote_address[0] == '@') {
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE), errmsg("remote not available")));
    }

    ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read page, file %s block %u from %s", relpath(rnode, fork_num),
                                                block_num, remote_address)));

    PROFILING_REMOTE_START();

    RepairBlockKey key;
    key.relfilenode = rnode.node;
    key.forknum = fork_num;
    key.blocknum = block_num;

    int ret_code = RemoteGetPage(remote_address, &key, BLCKSZ, cur_lsn, buf, pblk, timeout);

    PROFILING_REMOTE_END_READ(BLCKSZ, (ret_code == REMOTE_READ_OK));

    if (ret_code != REMOTE_READ_OK) {
        if (IS_DN_DUMMY_STANDYS_MODE() || retry_times >= 1) {
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE),
                            errmsg("remote read failed from %s, %s", remote_address, RemoteReadErrMsg(ret_code))));
        } else {
            ereport(WARNING,
                    (errmodule(MOD_REMOTE),
                     errmsg("remote read failed from %s, %s. try another", remote_address, RemoteReadErrMsg(ret_code)),
                     handle_in_client(true)));

            /* Check interrupts */
            CHECK_FOR_INTERRUPTS();

            remote_address = remote_address2;
            ++retry_times;
            goto retry; /* jump out  retry_times >= 1 */
        }
    }
}

/*
 * ForgetBuffer -- drop a buffer from shared buffers
 *
 * If the buffer isn't present in shared buffers, nothing happens.  If it is
 * present, it is discarded without making any attempt to write it back out to
 * the operating system.  The caller must therefore somehow be sure that the
 * data won't be needed for anything now or in the future.  It assumes that
 * there is no concurrent access to the block, except that it might be being
 * concurrently written.
 */
void ForgetBuffer(RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum)
{
    SMgrRelation smgr = smgropen(rnode, InvalidBackendId);
    BufferTag    tag;            /* identity of target block */
    uint32       hash;           /* hash value for tag */
    LWLock*      partitionLock;  /* buffer partition lock for it */
    int          bufId;
    BufferDesc  *bufHdr;
    uint32       bufState;

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(tag, smgr->smgr_rnode.node, forkNum, blockNum);

    /* determine its hash code and partition lock ID */
    hash = BufTableHashCode(&tag);
    partitionLock = BufMappingPartitionLock(hash);

    /* see if the block is in the buffer pool */
    LWLockAcquire(partitionLock, LW_SHARED);
    bufId = BufTableLookup(&tag, hash);
    LWLockRelease(partitionLock);

    /* didn't find it, so nothing to do */
    if (bufId < 0) {
        return;
    }

    /* take the buffer header lock */
    bufHdr = GetBufferDescriptor(bufId);
    bufState = LockBufHdr(bufHdr);

    /*
     * The buffer might been evicted after we released the partition lock and
     * before we acquired the buffer header lock.  If so, the buffer we've
     * locked might contain some other data which we shouldn't touch. If the
     * buffer hasn't been recycled, we proceed to invalidate it.
     */
    if (RelFileNodeEquals(bufHdr->tag.rnode, rnode) &&
        bufHdr->tag.blockNum == blockNum &&
        bufHdr->tag.forkNum == forkNum) {
        InvalidateBuffer(bufHdr);   /* releases spinlock */
    } else {
        UnlockBufHdr(bufHdr, bufState);
    }
}

bool InsertTdeInfoToCache(RelFileNode rnode, TdeInfo *tde_info)
{
    bool result = false;
    Assert(!TDE::TDEBufferCache::get_instance().empty());
    if (tde_info->algo == TDE_ALGO_NONE) {
        return false;
    }

    result = TDE::TDEBufferCache::get_instance().insert_cache(rnode, tde_info);
    return result;
}

void RelationInsertTdeInfoToCache(Relation reln)
{
    bool result = false;
    TdeInfo tde_info;

    GetTdeInfoFromRel(reln, &tde_info);
    /* insert tde info to cache */
    result = InsertTdeInfoToCache(reln->rd_node, &tde_info);
    if (!result) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("insert tde info to cache failed, relation RelFileNode is %u/%u/%u",
                   reln->rd_node.spcNode, reln->rd_node.dbNode, reln->rd_node.relNode),
            errdetail("N/A"),
            errcause("initialize cache memory failed"),
            erraction("check cache out of memory or system cache")));
    }
}

void PartitionInsertTdeInfoToCache(Relation reln, Partition p)
{
    bool result = false;
    TdeInfo tde_info;

    GetTdeInfoFromRel(reln, &tde_info);
    /* insert tde info to cache */
    result = InsertTdeInfoToCache(p->pd_node, &tde_info);
    if (!result) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("insert tde info to cache failed, partition RelFileNode is %u/%u/%u",
                   p->pd_node.spcNode, p->pd_node.dbNode, p->pd_node.relNode),
            errdetail("N/A"),
            errcause("initialize cache memory failed"),
            erraction("check cache out of memory or system cache")));
    }
}

