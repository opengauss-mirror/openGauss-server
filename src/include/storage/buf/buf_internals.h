/* -------------------------------------------------------------------------
 *
 * buf_internals.h
 *	  Internal definitions for buffer manager and the buffer replacement
 *	  strategy.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf/buf_internals.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUFMGR_INTERNALS_H
#define BUFMGR_INTERNALS_H

#include "storage/buf/buf.h"
#include "storage/buf/bufmgr.h"
#include "storage/latch.h"
#include "storage/lock/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr/smgr.h"
#include "storage/spin.h"
#include "utils/relcache.h"
#include "utils/atomic.h"
#include "access/xlogdefs.h"

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */

#define BUF_REFCOUNT_ONE 1LU
#define BUF_REFCOUNT_ONE_16 (1LU << 16)
#define BUF_REFCOUNT_ONE_32 (1LU << 32)
#define BUF_REFCOUNT_MASK ((1LU << 17) - 1)
#define BUF_USAGECOUNT_MASK 0x003C000000000000LU
#define BUF_USAGECOUNT_ONE (1LU << 18 << 32)
#define BUF_USAGECOUNT_SHIFT (18 + 32)
#define BUF_FLAG_MASK 0xFFC0000000000000LU

/* Get refcount and usagecount from buffer state */
#define BUF_STATE_GET_USAGECOUNT(state) (((state)&BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT)
#define BUF_STATE_GET_REFCOUNT(state) (((state)&0xFFFFFFFF))

/*
 * Flags for buffer descriptors
 *
 * Note: TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_IN_MIGRATE (1U << 16 << 32)        /* buffer is migrating */
#define BM_IS_TMP_BUF (1U << 21 << 32)         /* temp buf, can not write to disk */
#define BM_IS_META (1LU << 17 << 32)
#define BM_LOCKED (1LU << 22 << 32)            /* buffer header is locked */
#define BM_DIRTY (1LU << 23 << 32)             /* data needs writing */
#define BM_VALID (1LU << 24 << 32)             /* data is valid */
#define BM_TAG_VALID (1LU << 25 << 32)         /* tag is assigned */
#define BM_IO_IN_PROGRESS (1LU << 26 << 32)    /* read or write in progress */
#define BM_IO_ERROR (1LU << 27 << 32)          /* previous I/O failed */
#define BM_JUST_DIRTIED (1LU << 28 << 32)      /* dirtied since write started */
#define BM_PIN_COUNT_WAITER (1LU << 29 << 32)  /* have waiter for sole pin */
#define BM_CHECKPOINT_NEEDED (1LU << 30 << 32) /* must write for checkpoint */
#define BM_PERMANENT                      \
    (1LU << 31 << 32) /* permanent relation (not \
                * unlogged, or init fork) ) */
/*
 * The maximum allowed value of usage_count represents a tradeoff between
 * accuracy and speed of the clock-sweep buffer management algorithm.  A
 * large value (comparable to NBuffers) would approximate LRU semantics.
 * But it can take as many as BM_MAX_USAGE_COUNT+1 complete cycles of
 * clock sweeps to find a free buffer, so in practice we don't want the
 * value to be very large.
 */
#define BM_MAX_USAGE_COUNT 15

/*
 * Buffer tag identifies which disk block the buffer contains.
 *
 * Note: the BufferTag data must be sufficient to determine where to write the
 * block, without reference to pg_class or pg_tablespace entries.  It's
 * possible that the backend flushing the buffer doesn't even believe the
 * relation is visible yet (its xact may have started before the xact that
 * created the rel).  The storage manager must be able to cope anyway.
 *
 * Note: if there's any pad bytes in the struct, INIT_BUFFERTAG will have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct buftag {
    RelFileNode rnode; /* physical relation identifier */
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTag;

typedef struct buftagnocompress {
    RelFileNodeV2 rnode;
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTagSecondVer;


typedef struct buftagnohbkt {
    RelFileNodeOld rnode; /* physical relation identifier */
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTagFirstVer;

/* entry for buffer lookup hashtable */
typedef struct {
    BufferTag key; /* Tag of a disk page */
    int id;        /* Associated buffer ID */
} BufferLookupEnt;

#define CLEAR_BUFFERTAG(a)               \
    ((a).rnode.spcNode = InvalidOid,     \
        (a).rnode.dbNode = InvalidOid,   \
        (a).rnode.relNode = InvalidOid,  \
        (a).rnode.bucketNode = -1,\
        (a).rnode.opt = DefaultFileNodeOpt, \
        (a).forkNum = InvalidForkNumber, \
        (a).blockNum = InvalidBlockNumber)

#define INIT_BUFFERTAG(a, xx_rnode, xx_forkNum, xx_blockNum) \
    ((a).rnode = (xx_rnode), (a).forkNum = (xx_forkNum), (a).blockNum = (xx_blockNum))

#define BUFFERTAGS_EQUAL(a, b) \
    (RelFileNodeEquals((a).rnode, (b).rnode) && (a).blockNum == (b).blockNum && (a).forkNum == (b).forkNum)

#define BUFFERTAGS_PTR_EQUAL(a, b) \
    (RelFileNodeEquals((a)->rnode, (b)->rnode) && (a)->blockNum == (b)->blockNum && (a)->forkNum == (b)->forkNum)

#define BUFFERTAGS_PTR_SET(a, b)                 \
    ((a)->rnode.spcNode = (b)->rnode.spcNode,    \
        (a)->rnode.dbNode = (b)->rnode.dbNode,   \
        (a)->rnode.relNode = (b)->rnode.relNode, \
        (a)->rnode.bucketNode = (b)->rnode.bucketNode,\
        (a)->rnode.opt = (b)->rnode.opt,             \
        (a)->forkNum = (b)->forkNum,             \
        (a)->blockNum = (b)->blockNum)

/*
 * The shared buffer mapping table is partitioned to reduce contention.
 * To determine which partition lock a given tag requires, compute the tag's
 * hash code with BufTableHashCode(), then apply BufMappingPartitionLock().
 * NB: NUM_BUFFER_PARTITIONS must be a power of 2!
 */
#define BufTableHashPartition(hashcode) ((hashcode) % NUM_BUFFER_PARTITIONS)
#define BufMappingPartitionLock(hashcode) \
	(&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstBufMappingLock + \
		BufTableHashPartition(hashcode)].lock)
#define BufMappingPartitionLockByIndex(i) \
	(&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstBufMappingLock + (i)].lock)

/*
 *	BufferDesc -- shared descriptor/state data for a single shared buffer.
 *
 * Note: Buffer header lock (BM_LOCKED flag) must be held to examine or change
 * the tag, state or wait_backend_pid fields.  In general, buffer header lock
 * is a spinlock which is combined with flags, refcount and usagecount into
 * single atomic variable.  This layout allow us to do some operations in a
 * single atomic operation, without actually acquiring and releasing spinlock;
 * for instance, increase or decrease refcount.  buf_id field never changes
 * after initialization, so does not need locking. The LWLock can take care
 * of itself.  The buffer header lock is *not* used to control access to the
 * data in the buffer!
 *
 * It's assumed that nobody changes the state field while buffer header lock
 * is held.  Thus buffer header lock holder can do complex updates of the
 * state variable in single write, simultaneously with lock release (cleaning
 * BM_LOCKED flag).  On the other hand, updating of state without holding
 * buffer header lock is restricted to CAS, which insure that BM_LOCKED flag
 * is not set.  Atomic increment/decrement, OR/AND etc. are not allowed.
 *
 * An exception is that if we have the buffer pinned, its tag can't change
 * underneath us, so we can examine the tag without locking the buffer header.
 * Also, in places we do one-time reads of the flags without bothering to
 * lock the buffer header; this is generally for situations where we don't
 * expect the flag bit being tested to be changing.
 *
 * We can't physically remove items from a disk page if another backend has
 * the buffer pinned.  Hence, a backend may need to wait for all other pins
 * to go away.	This is signaled by storing its own PID into
 * wait_backend_pid and setting flag bit BM_PIN_COUNT_WAITER.  At present,
 * there can be only one such waiter per buffer.
 *
 * We use this same struct for local buffer headers, but the lock fields
 * are not used and not all of the flag bits are useful either.
 */
typedef struct BufferDescExtra {
    /* Cached physical location for segment-page storage, used for xlog */
    uint8 seg_fileno;
    BlockNumber seg_blockno;

    /* below fields are used for incremental checkpoint */
    pg_atomic_uint64 rec_lsn;        /* recovery LSN */
    volatile uint64 dirty_queue_loc; /* actual loc of dirty page queue */
    bool encrypt; /* enable table's level data encryption */

    volatile uint64 lsn_on_disk;

    volatile bool aio_in_progress; /* indicate aio is in progress */
} BufferDescExtra;

typedef struct BufferDesc {
    BufferTag tag; /* ID of page contained in buffer */

    /* state of the tag, containing flags, refcount and usagecount */
    pg_atomic_uint64 state;

    int buf_id;    /* buffer's index number (from 0) */

    ThreadId wait_backend_pid; /* backend PID of pin-count waiter */

    LWLock* io_in_progress_lock; /* to wait for I/O to complete */
    LWLock* content_lock;        /* to lock access to buffer contents */

    BufferDescExtra *extra;

#ifdef USE_ASSERT_CHECKING
    volatile uint64 lsn_dirty;
#endif
} BufferDesc;

/*
 * Concurrent access to buffer headers has proven to be more efficient if
 * they're cache line aligned. So we force the start of the BufferDescriptors
 * array to be on a cache line boundary and force the elements to be cache
 * line sized.
 *
 * XXX: As this is primarily matters in highly concurrent workloads which
 * probably all are 64bit these days, and the space wastage would be a bit
 * more noticeable on 32bit systems, we don't force the stride to be cache
 * line sized on those. If somebody does actual performance testing, we can
 * reevaluate.
 *
 * Note that local buffer descriptors aren't forced to be aligned - as there's
 * no concurrent access to those it's unlikely to be beneficial.
 *
 * We use 64bit as the cache line size here, because that's the most common
 * size. Making it bigger would be a waste of memory. Even if running on a
 * platform with either 32 or 128 byte line sizes, it's good to align to
 * boundaries and avoid false sharing.
 */
#define BUFFERDESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union BufferDescPadded {
    BufferDesc bufferdesc;
    char pad[BUFFERDESC_PAD_TO_SIZE];
} BufferDescPadded;

#define GetBufferDescriptor(id) (&t_thrd.storage_cxt.BufferDescriptors[(id)].bufferdesc)
#define GetLocalBufferDescriptor(id) ((BufferDesc *)&u_sess->storage_cxt.LocalBufferDescriptors[(id)].bufferdesc)
#define BufferDescriptorGetBuffer(bdesc) ((bdesc)->buf_id + 1)

#define BufferGetBufferDescriptor(buffer)                          \
    (AssertMacro(BufferIsValid(buffer)), BufferIsLocal(buffer) ?   \
        (BufferDesc *)&u_sess->storage_cxt.LocalBufferDescriptors[-(buffer)-1].bufferdesc : \
        &t_thrd.storage_cxt.BufferDescriptors[(buffer)-1].bufferdesc)

#define BufferDescriptorGetContentLock(bdesc) (((bdesc)->content_lock))
/*
 * Functions for acquiring/releasing a shared buffer header's spinlock.  Do
 * not apply these to local buffers!
 */
extern uint64 LockBufHdr(BufferDesc* desc);

#ifdef ENABLE_THREAD_CHECK
extern "C" {
    void AnnotateHappensBefore(const char *f, int l, uintptr_t addr);
}
#define TsAnnotateHappensBefore(addr)      AnnotateHappensBefore(__FILE__, __LINE__, (uintptr_t)addr)
#else
#define TsAnnotateHappensBefore(addr)
#endif

#define UnlockBufHdr(desc, s)                                    \
    do {                                                         \
        /* ENABLE_THREAD_CHECK only, release semantic */         \
        TsAnnotateHappensBefore(&desc->state);                   \
        pg_write_barrier();                                      \
        pg_atomic_write_u32((((volatile uint32 *)&(desc)->state) + 1), ( ( (s) & (~BM_LOCKED) ) >> 32) ); \
    } while (0)

#define FIX_SEG_BUFFER_TAG(node, tag, rel_node, block_num) \
    do {                                              \
        if (IsSegmentFileNode(node)) {                \
            tag.rnode.relnode = rel_node;              \
            tag.blocknum = block_num;                  \
            tag.rnode.bucketnode = SegmentBktId;      \
        }                                             \
    } while (0)

#define FIX_BUFFER_DESC(buf, pblk)              \
    do {                                            \
        Assert(PhyBlockIsValid(*pblk)); \
        buf->seg_fileno = pblk->rel_node;   \
        buf->seg_blockno = pblk->block;     \
        buf->seg_lsn = pblk->lsn;           \
    } while (0)

extern bool retryLockBufHdr(BufferDesc* desc, uint64* buf_state);
/*
 * The PendingWriteback & WritebackContext structure are used to keep
 * information about pending flush requests to be issued to the OS.
 */
typedef struct PendingWriteback {
    /* could store different types of pending flushes here */
    BufferTag tag;
} PendingWriteback;

/* struct forward declared in bufmgr.h */
typedef struct WritebackContext {
    /* pointer to the max number of writeback requests to coalesce */
    int* max_pending;

    /* current number of pending writeback requests */
    int nr_pending;

    /* pending requests */
    PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;

/* in bufmgr.c */

/*
 * Structure to sort buffers per file on checkpoints.
 *
 * This structure is allocated per buffer in shared memory, so it should be
 * kept as small as possible.
 */
typedef struct CkptSortItem {
    Oid tsId;
    Oid relNode;
    int2 bucketNode;
    ForkNumber forkNum;
    BlockNumber blockNum;
    int buf_id;
} CkptSortItem;

/*
 * Internal routines: only called by bufmgr
 */
/* bufmgr.c */
extern void WritebackContextInit(WritebackContext* context, int* max_pending);
extern void IssuePendingWritebacks(WritebackContext* context);
extern void ScheduleBufferTagForWriteback(WritebackContext* context, BufferTag* tag);

/* freelist.c */
extern BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy, uint64 *buf_state);

extern void StrategyFreeBuffer(volatile BufferDesc* buf);
extern bool StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc* buf);

extern int StrategySyncStart(uint32* complete_passes, uint32* num_buf_alloc);
extern void StrategyNotifyBgWriter(int bgwprocno);

extern Size StrategyShmemSize(void);
extern void StrategyInitialize(bool init);

extern BufferDesc *SSTryGetBuffer(uint64 times, uint64 *buf_state);
/* buf_table.c */
extern Size BufTableShmemSize(int size);
extern void InitBufTable(int size);
extern uint32 BufTableHashCode(BufferTag* tagPtr);
extern int BufTableLookup(BufferTag* tagPtr, uint32 hashcode);
extern int BufTableInsert(BufferTag* tagPtr, uint32 hashcode, int buf_id);
extern void BufTableDelete(BufferTag* tagPtr, uint32 hashcode);

/* localbuf.c */
extern void LocalPrefetchBuffer(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum);
extern BufferDesc* LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum, bool* foundPtr);
extern void MarkLocalBufferDirty(Buffer buffer);
extern void DropRelFileNodeLocalBuffers(const RelFileNode& rnode, ForkNumber forkNum, BlockNumber firstDelBlock);
extern void DropRelFileNodeAllLocalBuffers(const RelFileNode& rnode);
extern void AtEOXact_LocalBuffers(bool isCommit);
extern void update_wait_lockid(LWLock* lock);
extern char* PageDataEncryptForBuffer(Page page, BufferDesc *bufdesc, bool is_segbuf = false);
extern void FlushBuffer(void* buf, SMgrRelation reln, ReadBufferMethod flushmethod = WITH_NORMAL_CACHE, bool skipFsync = false);
extern void LocalBufferFlushAllBuffer();
#endif /* BUFMGR_INTERNALS_H */
