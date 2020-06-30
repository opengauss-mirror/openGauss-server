/* -------------------------------------------------------------------------
 *
 * freelist.cpp
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/buffer/freelist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/atomic.h"
#include "access/xlog.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "postmaster/aiocompleter.h" /* this is for the function AioCompltrIsReady() */
#include "access/double_write.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"

#define INT_ACCESS_ONCE(var) ((int)(*((volatile int*)&(var))))

/*
 * The shared freelist control information.
 */
typedef struct BufferStrategyControl {
    /* Spinlock: protects the values below */
    slock_t buffer_strategy_lock;

    /*
     * Clock sweep hand: index of next buffer to consider grabbing. Note that
     * this isn't a concrete buffer - we only ever increase the value. So, to
     * get an actual buffer, it needs to be used modulo NBuffers.
     */
    pg_atomic_uint32 nextVictimBuffer;

    /*
     * Statistics.	These counters should be wide enough that they can't
     * overflow during a single bgwriter cycle.
     */
    uint32 completePasses;            /* Complete cycles of the clock sweep */
    pg_atomic_uint32 numBufferAllocs; /* Buffers allocated since last reset */

    /*
     * Bgworker process to be notified upon activity or -1 if none. See
     * StrategyNotifyBgWriter.
     */
    int bgwprocno;
} BufferStrategyControl;

typedef struct
{
    int64  retry_times;
    int    cur_delay_time;
} StrategyDelayStatus;

const int MIN_DELAY_RETRY = 100;
const int MAX_DELAY_RETRY = 1000;
const float NEED_DELAY_RETRY_GET_BUF = 0.8;

/* Prototypes for internal functions */
static BufferDesc* GetBufferFromRing(BufferAccessStrategy strategy, uint32* buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy, volatile BufferDesc* buf);
void PageListBackWrite(uint32* bufList, int32 n,
    /* buffer list, bufs to scan, */
    uint32 flags = 0,                 /* opt flags */
    SMgrRelation use_smgrReln = NULL, /* opt relation */
    int32* bufs_written = NULL,       /* opt written count returned */
    int32* bufs_reusable = NULL);     /* opt reusable count returned */
static BufferDesc* getBufferFromFreeList(BufferAccessStrategy strategy, Dlelem **elt, uint32 *buf_state,
    BufFreeListHash **buf_list_entry);

static void perform_delay(StrategyDelayStatus *status)
{
    if (++(status->retry_times) > u_sess->attr.attr_storage.pagewriter_threshold &&
        get_dirty_page_num() > g_instance.attr.attr_storage.NBuffers * NEED_DELAY_RETRY_GET_BUF){
        if (status->cur_delay_time == 0) {
            status->cur_delay_time = MIN_DELAY_RETRY;
        }
        pg_usleep(status->cur_delay_time);

        /* increase delay by a random fraction between 1X and 2X */
        status->cur_delay_time += (int)(status->cur_delay_time * ((double)random() / (double)MAX_RANDOM_VALUE) + 0.5);
        if (status->cur_delay_time > MAX_DELAY_RETRY) {
            status->cur_delay_time = MIN_DELAY_RETRY;
        }
    }
    return;
}

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32 ClockSweepTick(int max_nbuffer_can_use)
{
    uint32 victim;

    /*
     * Atomically move hand ahead one buffer - if there's several processes
     * doing this, this can lead to buffers being returned slightly out of
     * apparent order.
     */
    victim = pg_atomic_fetch_add_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer, 1);
    if (victim >= (uint32)max_nbuffer_can_use) {
        uint32 original_victim = victim;

        /* always wrap what we look up in BufferDescriptors */
        victim = victim % max_nbuffer_can_use;

        /*
         * If we're the one that just caused a wraparound, force
         * completePasses to be incremented while holding the spinlock. We
         * need the spinlock so StrategySyncStart() can return a consistent
         * value consisting of nextVictimBuffer and completePasses.
         */
        if (victim == 0) {
            uint32 expected;
            uint32 wrapped;
            bool success = false;

            expected = original_victim + 1;

            while (!success) {
                /*
                 * Acquire the spinlock while increasing completePasses. That
                 * allows other readers to read nextVictimBuffer and
                 * completePasses in a consistent manner which is required for
                 * StrategySyncStart().  In theory delaying the increment
                 * could lead to a overflow of nextVictimBuffers, but that's
                 * highly unlikely and wouldn't be particularly harmful.
                 */
                SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);

                wrapped = expected % max_nbuffer_can_use;

                success = pg_atomic_compare_exchange_u32(
                    &t_thrd.storage_cxt.StrategyControl->nextVictimBuffer, &expected, wrapped);
                if (success)
                    t_thrd.storage_cxt.StrategyControl->completePasses++;
                SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
            }
        }
    }
    return victim;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held. 
 *
 *  If Standby, we restrict its memory usage to shared_buffers_fraction of
 *  NBuffers, Standby will not get buffer from freelist to avoid touching all
 *  buffers and always run the "clock sweep" in shared_buffers_fraction * NBuffers.
 *  If the fraction is too small, we will increase dynamiclly to avoid elog(ERROR)
 *  in `Startup' process because of ERROR will promote to FATAL.
 */
BufferDesc* StrategyGetBuffer(BufferAccessStrategy strategy, uint32* buf_state, Dlelem **buf_elt,
    BufFreeListHash **buf_list_entry)
{
    BufferDesc* buf = NULL;
    int bgwproc_no;
    int try_counter;
    uint32 local_buf_state = 0; /* to avoid repeated (de-)referencing */
    int max_buffer_can_use;
    bool am_standby = RecoveryInProgress();
    StrategyDelayStatus	retry_lock_status = {0, 0};
    StrategyDelayStatus	retry_buf_status = {0, 0};

    gstrace_entry(GS_TRC_ID_StrategyGetBuffer);

    /*
     * If given a strategy object, see whether it can select a buffer. We
     * assume strategy objects don't need buffer_strategy_lock.
     */
    if (strategy != NULL) {
        buf = GetBufferFromRing(strategy, buf_state);
        if (buf != NULL) {
            gstrace_exit(GS_TRC_ID_StrategyGetBuffer);
            return buf;
        }
    }

    /*
     * If asked, we need to waken the bgwriter. Since we don't want to rely on
     * a spinlock for this we force a read from shared memory once, and then
     * set the latch based on that value. We need to go through that length
     * because otherwise bgprocno might be reset while/after we check because
     * the compiler might just reread from memory.
     *
     * This can possibly set the latch of the wrong process if the bgwriter
     * dies in the wrong moment. But since PGPROC->procLatch is never
     * deallocated the worst consequence of that is that we set the latch of
     * some arbitrary process.
     */
    bgwproc_no = INT_ACCESS_ONCE(t_thrd.storage_cxt.StrategyControl->bgwprocno);
    if (bgwproc_no != -1) {
        /* reset bgwprocno first, before setting the latch */
        t_thrd.storage_cxt.StrategyControl->bgwprocno = -1;

        /*
         * Not acquiring ProcArrayLock here which is slightly icky. It's
         * actually fine because procLatch isn't ever freed, so we just can
         * potentially set the wrong process' (or no process') latch.
         */
        SetLatch(&g_instance.proc_base_all_procs[bgwproc_no]->procLatch);
    }

    /*
     * We count buffer allocation requests so that the bgwriter can estimate
     * the rate of buffer consumption.	Note that buffers recycled by a
     * strategy object are intentionally not counted here.
     */
    (void)pg_atomic_fetch_add_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 1);

    buf = getBufferFromFreeList(strategy, buf_elt, buf_state, buf_list_entry);
    if (buf != NULL) {
        gstrace_exit(GS_TRC_ID_StrategyGetBuffer);
        return buf;
    }

retry:
    /* Nothing on the freelist, so run the "clock sweep" algorithm */
    if (am_standby)
        max_buffer_can_use =
            int(g_instance.attr.attr_storage.NBuffers * u_sess->attr.attr_storage.shared_buffers_fraction);
    else
        max_buffer_can_use = g_instance.attr.attr_storage.NBuffers;
    try_counter = max_buffer_can_use;
    int try_get_loc_times = max_buffer_can_use;
    for (;;) {
        buf = GetBufferDescriptor(ClockSweepTick(max_buffer_can_use));
        /*
         * If the buffer is pinned, we cannot use it.
         */
        if (!retryLockBufHdr(buf, &local_buf_state)) {
            if (--try_get_loc_times == 0) {
                ereport(
                    WARNING, (errmsg("try get buf headr lock times equal to maxNBufferCanUse when StrategyGetBuffer")));
                try_get_loc_times = max_buffer_can_use;
            }
            perform_delay(&retry_lock_status);
            continue;
        }

        retry_lock_status.retry_times = 0;
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 &&
            (!dw_page_writer_running() || !(local_buf_state & BM_DIRTY))) {
            /* Found a usable buffer */
            if (strategy != NULL)
                AddBufferToRing(strategy, buf);
            *buf_state = local_buf_state;
            gstrace_exit(GS_TRC_ID_StrategyGetBuffer);
            return buf;
        } else if (--try_counter == 0) {
            /*
             * We've scanned all the buffers without making any state changes,
             * so all the buffers are pinned (or were when we looked at them).
             * We could hope that someone will free one eventually, but it's
             * probably better to fail than to risk getting stuck in an
             * infinite loop.
             */
            UnlockBufHdr(buf, local_buf_state);

            if (am_standby && u_sess->attr.attr_storage.shared_buffers_fraction < 1.0) {
                ereport(WARNING, (errmsg("no unpinned buffers available")));
                u_sess->attr.attr_storage.shared_buffers_fraction =
                    Min(u_sess->attr.attr_storage.shared_buffers_fraction + 0.1, 1.0);
                goto retry;
            } else if (dw_page_writer_running() &&
                       pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->page_writer_last_flush) > 0) {
                /*
                 * If the page_writer is still able to flush some buffers, we better
                 * retry (instead of giving up and throwing error).
                 */
                ereport(DEBUG3,
                    (errmsg("double writer is on, no buffer available, this buffer dirty is %u, "
                            "this buffer refcount is %u, now dirty page num is %ld",
                        (local_buf_state & BM_DIRTY),
                        BUF_STATE_GET_REFCOUNT(local_buf_state),
                        get_dirty_page_num())));
                perform_delay(&retry_buf_status);
                goto retry;
            } else if (t_thrd.storage_cxt.is_btree_split) {
                ereport(WARNING, (errmsg("no unpinned buffers available when btree insert parent")));
                goto retry;
            } else
                ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("no unpinned buffers available"))));
        }
        UnlockBufHdr(buf, local_buf_state);
        perform_delay(&retry_buf_status);
    }

    /* not reached */
    gstrace_exit(GS_TRC_ID_StrategyGetBuffer);
    return NULL;
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.	The alloc count is reset after
 * being read.
 */
int StrategySyncStart(uint32* complete_passes, uint32* num_buf_alloc)
{
    uint32 next_victim_buffer;
    int result;

    SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    next_victim_buffer = pg_atomic_read_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer);
    result = next_victim_buffer % g_instance.attr.attr_storage.NBuffers;

    if (complete_passes != NULL) {
        *complete_passes = t_thrd.storage_cxt.StrategyControl->completePasses;
        /*
         * Additionally add the number of wraparounds that happened before
         * completePasses could be incremented. C.f. ClockSweepTick().
         */
        *complete_passes += next_victim_buffer / (unsigned int)g_instance.attr.attr_storage.NBuffers;
    }

    if (num_buf_alloc != NULL) {
        *num_buf_alloc = pg_atomic_exchange_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 0);
    }
    SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwriterLatch isn't NULL, the next invocation of StrategyGetBuffer will
 * set that latch.	Pass NULL to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void StrategyNotifyBgWriter(int bgwproc_no)
{
    /*
     * We acquire the BufFreelistLock just to ensure that the store appears
     * atomic to StrategyGetBuffer.  The bgwriter should call this rather
     * infrequently, so there's no performance penalty from being safe.
     */
    SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    t_thrd.storage_cxt.StrategyControl->bgwprocno = bgwproc_no;
    SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
}

/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size StrategyShmemSize(void)
{
    Size size = 0;

    /* size of lookup hash table ... see comment in StrategyInitialize */
    size = add_size(size, BufTableShmemSize(g_instance.attr.attr_storage.NBuffers + NUM_BUFFER_PARTITIONS));

    /* size of the shared replacement strategy control block */
    size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

    return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void StrategyInitialize(bool init)
{
    bool found = false;

    /*
     * Initialize the shared buffer lookup hashtable.
     *
     * Since we can't tolerate running out of lookup table entries, we must be
     * sure to specify an adequate table size here.  The maximum steady-state
     * usage is of course NBuffers entries, but BufferAlloc() tries to insert
     * a new entry before deleting the old.  In principle this could be
     * happening in each partition concurrently, so we could need as many as
     * NBuffers + NUM_BUFFER_PARTITIONS entries.
     */
    InitBufTable(g_instance.attr.attr_storage.NBuffers + NUM_BUFFER_PARTITIONS);

    /*
     * Get or create the shared strategy control block
     */
    t_thrd.storage_cxt.StrategyControl =
        (BufferStrategyControl*)ShmemInitStruct("Buffer Strategy Status", sizeof(BufferStrategyControl), &found);

    if (!found) {
        /*
         * Only done once, usually in postmaster
         */
        Assert(init);
        SpinLockInit(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);

        /* Initialize the clock sweep pointer */
        pg_atomic_init_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer, 0);

        /* Clear statistics */
        t_thrd.storage_cxt.StrategyControl->completePasses = 0;
        pg_atomic_init_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 0);

        /* No pending notification */
        t_thrd.storage_cxt.StrategyControl->bgwprocno = -1;
    } else {
        Assert(!init);
    }
}

/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */
/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy GetAccessStrategy(BufferAccessStrategyType btype)
{
    BufferAccessStrategy strategy;
    int ring_size;

    /*
     * Select ring size to use.  See buffer/README for rationales.
     *
     * Note: if you change the ring size for BAS_BULKREAD, see also
     * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
     */
    switch (btype) {
        case BAS_NORMAL:
            /* if someone asks for NORMAL, just give 'em a "default" object */
            return NULL;

        case BAS_BULKREAD:
            ring_size = int(int64(u_sess->attr.attr_storage.bulk_read_ring_size) * 1024 / BLCKSZ);
            break;
        case BAS_BULKWRITE:
            ring_size = (u_sess->attr.attr_storage.bulk_write_ring_size / BLCKSZ) * 1024;
            break;
        case BAS_VACUUM:
            ring_size = 256 * 1024 / BLCKSZ;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION), (errmsg("unrecognized buffer access strategy: %d", (int)btype))));
            return NULL; /* keep compiler quiet */
    }

    /* Make sure ring isn't an undue fraction of shared buffers */
    if (btype != BAS_BULKWRITE && btype != BAS_BULKREAD)
        ring_size = Min(g_instance.attr.attr_storage.NBuffers / 8, ring_size);
    else
        ring_size = Min(g_instance.attr.attr_storage.NBuffers / 4, ring_size);

    /* Allocate the object and initialize all elements to zeroes */
    strategy = (BufferAccessStrategy)palloc0(offsetof(BufferAccessStrategyData, buffers) + ring_size * sizeof(Buffer));

    /* Set fields that don't start out zero */
    strategy->btype = btype;
    strategy->ring_size = ring_size;
    strategy->flush_rate = Min(u_sess->attr.attr_storage.backwrite_quantity, ring_size);

    return strategy;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void FreeAccessStrategy(BufferAccessStrategy strategy)
{
    /* don't crash if called on a "default" strategy */
    if (strategy != NULL) {
        pfree(strategy);
        strategy = NULL;
    }
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc* GetBufferFromRing(BufferAccessStrategy strategy, uint32* buf_state)
{
    BufferDesc* buf = NULL;
    Buffer buf_num;
    uint32 local_buf_state; /* to avoid repeated (de-)referencing */

    /* Advance to next ring slot */
    if (++strategy->current >= strategy->ring_size)
        strategy->current = 0;

    ADIO_RUN()
    {
        /*
         * Flush out buffers asynchronously from behind the current slot.
         * This is a kludge because the PageListBackWrite() is not strictly
         * asynchronous and this function really shouldn't be doing the actual I/O.
         */
        if (AioCompltrIsReady() &&
            ((strategy->btype == BAS_BULKWRITE) && (strategy->current % strategy->flush_rate == 0))) {
            if (strategy->current == 0) {
                if (strategy->buffers[strategy->ring_size - strategy->flush_rate] != InvalidBuffer) {
                    PageListBackWrite((uint32*)&strategy->buffers[strategy->ring_size - strategy->flush_rate],
                        strategy->flush_rate,
                        STRATEGY_BACKWRITE,
                        NULL,
                        NULL,
                        NULL);
                    ereport(DEBUG1,
                        (errmodule(MOD_ADIO),
                            errmsg("BufferRingBackWrite, start(%d) count(%d)",
                                strategy->buffers[strategy->ring_size - strategy->flush_rate],
                                strategy->flush_rate)));
                }
            } else {
                PageListBackWrite((uint32*)&strategy->buffers[strategy->current - strategy->flush_rate],
                    strategy->flush_rate,
                    STRATEGY_BACKWRITE,
                    NULL,
                    NULL,
                    NULL);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("BufferRingBackWrite, start(%d) count(%d)",
                            strategy->buffers[strategy->current - strategy->flush_rate],
                            strategy->flush_rate)));
            }
        }
    }
    ADIO_END();

    /*
     * If the slot hasn't been filled yet, tell the caller to allocate a new
     * buffer with the normal allocation strategy.	He will then fill this
     * slot by calling AddBufferToRing with the new buffer.
     */
    buf_num = strategy->buffers[strategy->current];
    if (buf_num == InvalidBuffer) {
        strategy->current_was_in_ring = false;
        return NULL;
    }

    /*
     * If the buffer is pinned we cannot use it under any circumstances.
     *
     * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
     * since our own previous usage of the ring element would have left it
     * there, but it might've been decremented by clock sweep since then). A
     * higher usage_count indicates someone else has touched the buffer, so we
     * shouldn't re-use it.
     */
    buf = GetBufferDescriptor(buf_num - 1);
    local_buf_state = LockBufHdr(buf);
    if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1 &&
        (!dw_page_writer_running() || !(local_buf_state & BM_DIRTY))) {
        strategy->current_was_in_ring = true;
        *buf_state = local_buf_state;
        return buf;
    }
    UnlockBufHdr(buf, local_buf_state);
    /*
     * Tell caller to allocate a new buffer with the normal allocation
     * strategy.  He'll then replace this ring element via AddBufferToRing.
     */
    strategy->current_was_in_ring = false;
    return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void AddBufferToRing(BufferAccessStrategy strategy, volatile BufferDesc* buf)
{
    strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.	This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc* buf)
{
    /* We only do this in bulkread mode */
    if (strategy->btype != BAS_BULKREAD)
        return false;

    /* Don't muck with behavior of normal buffer-replacement strategy */
    if (!strategy->current_was_in_ring || strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
        return false;

    /*
     * Remove the dirty buffer from the ring; necessary to prevent infinite
     * loop if all ring members are dirty.
     */
    strategy->buffers[strategy->current] = InvalidBuffer;

    return true;
}

void StrategyGetRingPrefetchQuantityAndTrigger(BufferAccessStrategy strategy, int* quantity, int* trigger)
{
    int threshold;
    int prefetch_trigger = u_sess->attr.attr_storage.prefetch_quantity;

    if (strategy == NULL || strategy->btype != BAS_BULKREAD) {
        return;
    }
    threshold = strategy->ring_size / 4;
    if (quantity != NULL) {
        *quantity = (threshold > u_sess->attr.attr_storage.prefetch_quantity)
                        ? u_sess->attr.attr_storage.prefetch_quantity
                        : threshold;
    }
    if (trigger != NULL) {
        *trigger = (threshold > prefetch_trigger) ? prefetch_trigger : threshold;
    }
}

static inline void getKeyAndListNum(int *buf_free_list_num, int *key)
{
    if (g_instance.attr.attr_storage.enableIncrementalCheckpoint) {
        *buf_free_list_num = NUM_BUFFER_FREE_LIST;
        *key = free_list_random() % *buf_free_list_num;
    } else {
        *key = 0;
        *buf_free_list_num = 1;
    }
}

/**
 * @Description: Get one buffer from buffer free list. To ensure that no one else can pin the buffer before we do,
 *            we must return the buffer with the buffer header spinlock still held.
 */
static BufferDesc* getBufferFromFreeList(BufferAccessStrategy strategy, Dlelem **buf_elt, uint32 *buf_state,
    BufFreeListHash **buf_list_entry_find)
{
    BufFreeListHash *buf_list_entry = NULL;
    BufListElem     *buf_entry = NULL;
    BufferDesc      *buf = NULL;
    int             key = 0;
    bool            found = false;
    int             retry_times = 0;
    int             buf_free_list_num = 0;

    getKeyAndListNum(&buf_free_list_num, &key);

    while (retry_times++ < buf_free_list_num) {
        buf_list_entry =
                    (BufFreeListHash*)hash_search(t_thrd.storage_cxt.BufFreeListHash, (void*)&key, HASH_FIND, &found);
        /* If this buffer free list does not have any buffer, choose the next free list except the first free list. */
        if (buf_list_entry->buf_free_num <= 0) {
            key = free_list_random() % buf_free_list_num;
            continue;
        }
        Dlelem *buf_elt_next = NULL;

        (void)LWLockAcquire(buf_list_entry->lock, LW_SHARED);
        for (*buf_elt = DLGetHead(&buf_list_entry->buf_free_Dllist); *buf_elt; *buf_elt = buf_elt_next) {
            buf_elt_next = DLGetSucc(*buf_elt);

            buf_entry = (BufListElem*)DLE_VAL(*buf_elt);
            buf = GetBufferDescriptor(buf_entry->buf_id);
            if (!retryLockBufHdr(buf, buf_state)) {
                continue;
            }

            if (BUF_STATE_GET_REFCOUNT(*buf_state) == 0 &&
                (!dw_page_writer_running() || !(*buf_state & BM_DIRTY))) {
                LWLockRelease(buf_list_entry->lock);

                buf->free_list_idx = -1;
                if (strategy != NULL) {
                    AddBufferToRing(strategy, buf);
                }
                /* return this buffer to BufferAlloc, after release the buffer spinlock, remove the buf form list. */
                *buf_list_entry_find = buf_list_entry;
                return buf;
            }

            UnlockBufHdr(buf, *buf_state);
        }
        LWLockRelease(buf_list_entry->lock);
        key = free_list_random() % buf_free_list_num;
    }

    return NULL;
}

void InitBufFreeTable(int size)
{
    HASHCTL hashctl; 

    hashctl.keysize = sizeof(int);
    hashctl.entrysize = sizeof(BufFreeListHash);
    hashctl.hash = tag_hash;

    t_thrd.storage_cxt.BufFreeListHash = ShmemInitHash("Shared Buffer Free Table", size, size, &hashctl,
        HASH_ELEM | HASH_FUNCTION);
}

static void pushBufFreeList(BufFreeListHash *buf_list_entry, int buf_id, int list_idx)
{
    BufListElem *buf_entry = NULL;
    BufferDesc  *buf = NULL;
    Dlelem      *elt = NULL;

    buf_entry = (BufListElem *)palloc(sizeof(BufListElem));
    buf_entry->buf_id = buf_id;
    buf = GetBufferDescriptor(buf_id);
    buf->free_list_idx = list_idx;
    elt = DLNewElem((void*)buf_entry);
    DLAddTail(&buf_list_entry->buf_free_Dllist, elt);
    buf_list_entry->buf_free_num++;

    return;
}

/**
 * @Description: Add all buffer to the buffer free list evenly.
 */
void InitBufFreeList()
{
    bool    found = false;
    int     list_idx;
    int     buf_id;
    BufFreeListHash *buf_list_entry = NULL;
    int     list_num = g_instance.attr.attr_storage.enableIncrementalCheckpoint ? NUM_BUFFER_FREE_LIST : 1;
    int     avg_buf_num = g_instance.attr.attr_storage.NBuffers / list_num;

    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

    for (list_idx = 0; list_idx < list_num; list_idx++) {
        buf_list_entry = 
            (BufFreeListHash*)hash_search(t_thrd.storage_cxt.BufFreeListHash, (void*)&list_idx, HASH_ENTER, &found);
        INIT_BUF_FREE_LIST_ENTRY(buf_list_entry);

        (void)LWLockAcquire(buf_list_entry->lock, LW_EXCLUSIVE);
        for (buf_id = list_idx * avg_buf_num; buf_id < (list_idx + 1) * avg_buf_num; buf_id++) {
            pushBufFreeList(buf_list_entry, buf_id, list_idx);
        }
        LWLockRelease(buf_list_entry->lock);
    }

    /* If there are remaining pages in the buffer pool, put them to first freelist. */
    list_idx = 0;
    buf_list_entry = 
            (BufFreeListHash*)hash_search(t_thrd.storage_cxt.BufFreeListHash, (void*)&list_idx, HASH_FIND, &found);

    (void)LWLockAcquire(buf_list_entry->lock, LW_EXCLUSIVE);
    for (buf_id = list_num * avg_buf_num; buf_id < g_instance.attr.attr_storage.NBuffers; buf_id++) {
        pushBufFreeList(buf_list_entry, buf_id, list_idx);
    }
    LWLockRelease(buf_list_entry->lock);
    (void)MemoryContextSwitchTo(oldcontext);
}

bool need_push_buffer_free_list(BufferDesc *bufhdr, int32 key)
{
    __uint64_t compare = 0;
    __uint64_t exchange = 0;
    __uint64_t current = 0;
    int32 free_list_idx = 0;
    uint32 state;
    errno_t rc;

    compare = __sync_val_compare_and_swap((__uint64_t*)&bufhdr->state, compare, exchange);
loop:
    rc = memcpy_s(&free_list_idx, sizeof(int32), (char*)&compare + sizeof(uint32), sizeof(int32));
    securec_check(rc, "", "");
    if (free_list_idx != -1) {
        return false;
    }
    pg_read_barrier();
    rc = memcpy_s(&state, sizeof(uint32), (char*)&compare, sizeof(uint32));
    securec_check(rc, "", "");
    if (state & BM_LOCKED) {
        current = __sync_val_compare_and_swap((__uint64_t*)&bufhdr->state, compare, compare);
        compare = current;
        goto loop;
    }
    free_list_idx = key;
    rc = memcpy_s(&exchange, sizeof(uint32), &compare, sizeof(uint32));
    securec_check(rc, "", "");
    rc = memcpy_s((char*)&exchange + sizeof(uint32), sizeof(int32), &free_list_idx, sizeof(int32));
    securec_check(rc, "", "");

    current = __sync_val_compare_and_swap((__uint64_t*)&bufhdr->state, compare, exchange);
    if (compare != current) {
        compare = current;
        goto loop;
    }
    return true;
}

/**
 * @Description: After InvalidateBuffer, add the buffer to the first buffer free list.
 * @in: buffer header
 */
void AddBufToFreeList(BufferDesc *buf)
{
    Dlelem* elt = NULL;
    BufFreeListHash *buf_list_entry = NULL;
    BufListElem *buf_entry = NULL;
    bool found = false;
    int key = 0;

    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

    buf_list_entry = (BufFreeListHash*)hash_search(t_thrd.storage_cxt.BufFreeListHash, (void*)&key, HASH_ENTER, &found);
    if (buf_list_entry->buf_free_num > g_instance.attr.attr_storage.NBuffers) {
        return;
    }

    if (need_push_buffer_free_list(buf, 0)) {
        buf_entry = (BufListElem *)palloc(sizeof(BufListElem));
        buf_entry->buf_id = buf->buf_id;
        elt = DLNewElem((void*)buf_entry);

        (void)LWLockAcquire(buf_list_entry->lock, LW_EXCLUSIVE);
        DLAddTail(&buf_list_entry->buf_free_Dllist, elt);
        buf_list_entry->buf_free_num++;
        LWLockRelease(buf_list_entry->lock);
    }
    (void)MemoryContextSwitchTo(oldcontext);
}

static BufFreeListHash* getNextFreeList()
{
    uint64  key;
    bool    found = false;
    BufFreeListHash *buf_list_entry = NULL;

    key = free_list_random() % NUM_BUFFER_FREE_LIST;
    buf_list_entry =
        (BufFreeListHash*)hash_search(t_thrd.storage_cxt.BufFreeListHash, (void*)&key, HASH_FIND, &found);
    return buf_list_entry;
}

const int RETRY_GET_NEXT_LIST = 10;
const int RETRY_GET_LIST_LOCK = 5;

void pushBufToList(BufFreeListHash *buf_list_entry, int start_loc, int end_loc)
{
    BufListElem     *buf_entry = NULL;
    int             buf_id;
    Dlelem          *elt = NULL;
    BufferDesc      *bufhdr = NULL;
    int             retry_times = 0;

    buf_list_entry = getNextFreeList();
    while (buf_list_entry->buf_free_num >= g_instance.attr.attr_storage.NBuffers / NUM_BUFFER_FREE_LIST
        && retry_times++ < RETRY_GET_NEXT_LIST) {
        buf_list_entry = getNextFreeList();
    }

    retry_times = 0;

    while (!LWLockConditionalAcquire(buf_list_entry->lock, LW_EXCLUSIVE)) {
        if (retry_times++ >= RETRY_GET_LIST_LOCK) {
            buf_list_entry = getNextFreeList();
            retry_times = 0;
        }
    }

    for (int i = start_loc; i <= end_loc; i++) {
        buf_id = g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id;
        if (buf_id == DW_INVALID_BUFFER_ID) {
            continue;
        }
        bufhdr = GetBufferDescriptor(buf_id);
        if (need_push_buffer_free_list(bufhdr, buf_list_entry->key)) {
            buf_entry = (BufListElem *)palloc(sizeof(BufListElem));
            buf_entry->buf_id = buf_id;
            elt = DLNewElem((void*)buf_entry);
            DLAddTail(&buf_list_entry->buf_free_Dllist, elt);
            buf_list_entry->buf_free_num++;
        }
    }
    LWLockRelease(buf_list_entry->lock);
}

const int BATCH_ADD_FREE_LIST_NUM = 5;
/**
 * @Description: pagewriter thread flush the buffer to data file, add these buffer to free list,
 *            except for the first one.
 */
void AddBatchBufToFreeList(int thread_id)
{
    BufFreeListHash *buf_list_entry = NULL;
    MemoryContext   oldcontext = NULL;
    int start = g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_id].start_loc;
    int end = g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_id].end_loc;
    int avg_num = (end - start) / BATCH_ADD_FREE_LIST_NUM;
    int remain_num = (end - start) % BATCH_ADD_FREE_LIST_NUM;
    int temp_start;
    int temp_end;

    oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

    for (int i = 0; i < BATCH_ADD_FREE_LIST_NUM; i++) {
        if (i == 0) {
            temp_start = start;
            temp_end = start + avg_num + remain_num - 1;
        } else {
            temp_start = start + avg_num * i + remain_num;
            temp_end = temp_start + avg_num - 1;
        }
        pushBufToList(buf_list_entry, temp_start, temp_end);
    }
    
    (void)MemoryContextSwitchTo(oldcontext);
}

/**
 * @Description: Remove the buf from the buffer free list after release buffer desc spinlock.
 * @in: buffer free list
 * @in: list element need remove.
 */
void RemoveBufFromFreeList(BufferDesc *buf, BufFreeListHash *buf_list_entry, Dlelem *elt)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

    (void)LWLockAcquire(buf_list_entry->lock, LW_EXCLUSIVE);
    if (elt != NULL) {
        DLRemove(elt);
        buf_list_entry->buf_free_num--;
        pfree(DLE_VAL(elt));
        DLFreeElem(elt);
    }
    LWLockRelease(buf_list_entry->lock);
    (void)MemoryContextSwitchTo(oldcontext);
}
