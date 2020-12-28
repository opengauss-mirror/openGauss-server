/* -------------------------------------------------------------------------
 *
 * bufmgr.h
 *	  POSTGRES buffer manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf/bufmgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUFMGR_H
#define BUFMGR_H

#include "knl/knl_variable.h"
#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufpage.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

typedef void* Block;

typedef struct PrivateRefCountEntry {
    Buffer buffer;
    int32 refcount;
} PrivateRefCountEntry;

/* 64 bytes, about the size of a cache line on common systems */
#define REFCOUNT_ARRAY_ENTRIES 8

/* Possible arguments for GetAccessStrategy() */
typedef enum BufferAccessStrategyType {
    BAS_NORMAL,    /* Normal random access */
    BAS_BULKREAD,  /* Large read-only scan (hint bit updates are
                    * ok) */
    BAS_BULKWRITE, /* Large multi-block write (e.g. COPY IN) */
    BAS_VACUUM     /* VACUUM */
} BufferAccessStrategyType;

/* Possible modes for ReadBufferExtended() */
typedef enum {
    RBM_NORMAL,                /* Normal read */
    RBM_ZERO_AND_LOCK,         /* Don't read from disk, caller will
                                * initialize */
    RBM_ZERO_AND_CLEANUP_LOCK, /* Like RBM_ZERO_AND_LOCK, but locks the page
                                * in "cleanup" mode */
    RBM_ZERO_ON_ERROR,         /* Read, but return an all-zeros page on error */
    RBM_NORMAL_NO_LOG,         /* Don't log page as invalid during WAL
                                * replay; otherwise same as RBM_NORMAL */
    RBM_FOR_REMOTE             /* Like RBM_NORMAL, but not remote read again when PageIsVerified failed. */
} ReadBufferMode;

typedef enum
{
	WITH_NORMAL_CACHE = 0,		/* Normal read */
    WITH_LOCAL_CACHE,       /* Add for batchredo , the same as localbuf */
    WITH_OUT_CACHE          /* without buf, read or write directly */
} ReadBufferMethod;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData {
    /* Overall strategy type */
    BufferAccessStrategyType btype;
    /* Number of elements in buffers[] array */
    int ring_size;

    /*
     * Index of the "current" slot in the ring, ie, the one most recently
     * returned by GetBufferFromRing.
     */
    int current;

    /* Number of elements to flush behind current */
    int flush_rate;

    /*
     * True if the buffer just returned by StrategyGetBuffer had been in the
     * ring already.
     */
    bool current_was_in_ring;

    /*
     * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
     * have not yet selected a buffer for this ring slot.  For allocation
     * simplicity this is palloc'd together with the fixed fields of the
     * struct.
     */
    Buffer buffers[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE SIZE ARRAY */
} BufferAccessStrategyData;

/* forward declared, to avoid having to expose buf_internals.h here */
struct WritebackContext;

/* special block number for ReadBuffer() */
#define P_NEW InvalidBlockNumber /* grow the file to get a new page */

/* Bits in SyncOneBuffer's return value */
#define BUF_WRITTEN 0x01
#define BUF_REUSABLE 0x02
#define BUF_SKIPPED 0x04

/*
 * Buffer content lock modes (mode argument for LockBuffer())
 */
#define BUFFER_LOCK_UNLOCK 0
#define BUFFER_LOCK_SHARE 1
#define BUFFER_LOCK_EXCLUSIVE 2

#define MAX_PREFETCH_REQSIZ 512
#define MAX_BACKWRITE_REQSIZ 64

/*
 * BufferIsPinned
 *		True iff the buffer is pinned (also checks for valid buffer number).
 *
 *		NOTE: what we check here is that *this* backend holds a pin on
 *		the buffer.  We do not care whether some other backend does.
 */
#define BufferIsPinned(bufnum)                                                                            \
    (!BufferIsValid(bufnum) ? false                                                                       \
                            : BufferIsLocal(bufnum) ? (u_sess->storage_cxt.LocalRefCount[-(bufnum)-1] > 0) \
                                                    : (GetPrivateRefCount(bufnum) > 0))

/*
 * These routines are beaten on quite heavily, hence the macroization.
 */

/*
 * BufferIsValid
 *		True iff the given buffer number is valid (either as a shared
 *		or local buffer).
 *
 * Note: For a long time this was defined the same as BufferIsPinned,
 * that is it would say False if you didn't hold a pin on the buffer.
 * I believe this was bogus and served only to mask logic errors.
 * Code should always know whether it has a buffer reference,
 * independently of the pin state.
 *
 * Note: For a further long time this was not quite the inverse of the
 * BufferIsInvalid() macro, in that it also did sanity checks to verify
 * that the buffer number was in range.  Most likely, this macro was
 * originally intended only to be used in assertions, but its use has
 * since expanded quite a bit, and the overhead of making those checks
 * even in non-assert-enabled builds can be significant.  Thus, we've
 * now demoted the range checks to assertions within the macro itself.
 */
#define BufferIsValid(bufnum)                                                                                      \
    (AssertMacro((bufnum) <= g_instance.attr.attr_storage.NBuffers && (bufnum) >= -u_sess->storage_cxt.NLocBuffer), \
        (bufnum) != InvalidBuffer)

/*
 * BufferGetBlock
 *		Returns a reference to a disk page image associated with a buffer.
 *
 * Note:
 *		Assumes buffer is valid.
 */
#define BufferGetBlock(buffer)                                                           \
    (AssertMacro(BufferIsValid(buffer)),                                                 \
        BufferIsLocal(buffer) ? u_sess->storage_cxt.LocalBufferBlockPointers[-(buffer)-1] \
                              : (Block)(t_thrd.storage_cxt.BufferBlocks + ((Size)((uint)(buffer)-1)) * BLCKSZ))

/*
 * BufferGetPageSize
 *		Returns the page size within a buffer.
 *
 * Notes:
 *		Assumes buffer is valid.
 *
 *		The buffer can be a raw disk block and need not contain a valid
 *		(formatted) disk page.
 */
/* XXX should dig out of buffer descriptor */
#define BufferGetPageSize(buffer) (AssertMacro(BufferIsValid(buffer)), (Size)BLCKSZ)

/*
 * BufferGetPage
 *		Returns the page associated with a buffer.
 */
#define BufferGetPage(buffer) ((Page)BufferGetBlock(buffer))

/* Note: these two macros only work on shared buffers, not local ones! */
#define BufHdrGetBlock(bufHdr) ((Block)(t_thrd.storage_cxt.BufferBlocks + ((Size)(uint32)(bufHdr)->buf_id) * BLCKSZ))
#define BufferGetLSN(bufHdr) (PageGetLSN(BufHdrGetBlock(bufHdr)))

/* Note: this macro only works on local buffers, not shared ones! */
#define LocalBufHdrGetBlock(bufHdr) u_sess->storage_cxt.LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]
#define LocalBufGetLSN(bufHdr) (PageGetLSN(LocalBufHdrGetBlock(bufHdr)))

/*
 * prototypes for functions in bufmgr.c
 */
extern void PrefetchBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum);
extern void PageRangePrefetch(
    Relation reln, ForkNumber forkNum, BlockNumber blockNum, int32 n, uint32 flags, uint32 col);
extern void PageListPrefetch(
    Relation reln, ForkNumber forkNum, BlockNumber* blockList, int32 n, uint32 flags, uint32 col);
extern Buffer ReadBuffer(Relation reln, BlockNumber blockNum);
extern Buffer ReadBufferExtended(
    Relation reln, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode, BufferAccessStrategy strategy);
extern Buffer ReadBufferWithoutRelcache(
    const RelFileNode& rnode, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode, BufferAccessStrategy strategy);
extern Buffer ReadBufferForRemote(const RelFileNode& rnode, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode,
    BufferAccessStrategy strategy, bool* hit);

extern void ReleaseBuffer(Buffer buffer);
extern void UnlockReleaseBuffer(Buffer buffer);
extern void MarkBufferDirty(Buffer buffer);
extern void IncrBufferRefCount(Buffer buffer);
extern Buffer ReleaseAndReadBuffer(Buffer buffer, Relation relation, BlockNumber blockNum);

extern void InitBufferPool(void);
extern void InitBufferPoolAccess(void);
extern void InitBufferPoolBackend(void);
extern void AtEOXact_Buffers(bool isCommit);
extern void PrintBufferLeakWarning(Buffer buffer);
extern void CheckPointBuffers(int flags, bool doFullCheckpoint);
extern BlockNumber BufferGetBlockNumber(Buffer buffer);
extern BlockNumber RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber forkNum, bool estimate = false);
extern void FlushRelationBuffers(Relation rel, HTAB *hashtbl = NULL);
extern void FlushDatabaseBuffers(Oid dbid);

extern void DropRelFileNodeBuffers(const RelFileNodeBackend& rnode, ForkNumber forkNum, BlockNumber firstDelBlock);
extern void DropTempRelFileNodeAllBuffers(const RelFileNodeBackend& rnode);


#define DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD 20
#define IS_DEL_RELS_OVER_HASH_THRESHOLD(ndelrels) ((ndelrels) > DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD)

extern void DropRelFileNodeAllBuffersUsingHash(HTAB* relfilenode_hashtbl);
extern void DropRelFileNodeAllBuffersUsingScan(RelFileNode* rnode, int rnode_len);

extern void DropDatabaseBuffers(Oid dbid);

extern BlockNumber PartitionGetNumberOfBlocksInFork(Relation relation, Partition partition, ForkNumber forkNum);

#define RelationGetNumberOfBlocks(reln) RelationGetNumberOfBlocksInFork(reln, MAIN_FORKNUM)

/*
 * PartitionGetNumberOfBlocks
 *		Determines the current number of pages in the partition.
 */
#define PartitionGetNumberOfBlocks(rel, part) PartitionGetNumberOfBlocksInFork(rel, part, MAIN_FORKNUM)

extern bool BufferIsPermanent(Buffer buffer);

extern void RemoteReadBlock(const RelFileNodeBackend& rnode, ForkNumber forkNum, BlockNumber blockNum, char* buf);

#ifdef NOT_USED
extern void PrintPinnedBufs(void);
#endif
extern Size BufferShmemSize(void);
extern void BufferGetTag(Buffer buffer, RelFileNode* rnode, ForkNumber* forknum, BlockNumber* blknum);

void PinBuffer_Locked(volatile BufferDesc* buf);
void UnpinBuffer(BufferDesc* buf, bool fixOwner);
extern void MarkBufferDirtyHint(Buffer buffer, bool buffer_std);
extern void FlushOneBuffer(Buffer buffer);
extern void UnlockBuffers(void);
extern void LockBuffer(Buffer buffer, int mode);
extern bool ConditionalLockBuffer(Buffer buffer);
extern void LockBufferForCleanup(Buffer buffer);
extern bool ConditionalLockBufferForCleanup(Buffer buffer);
extern bool HoldingBufferPinThatDelaysRecovery(void);
extern void AsyncUnpinBuffer(volatile void* bufHdr, bool forgetBuffer);
extern void AsyncCompltrPinBuffer(volatile void* bufHdr);
extern void AsyncCompltrUnpinBuffer(volatile void* bufHdr);
extern void TerminateBufferIO(volatile BufferDesc* buf, bool clear_dirty, uint32 set_flag_bits);

extern void AsyncTerminateBufferIO(void* bufHdr, bool clear_dirty, uint32 set_flag_bits);
extern void AsyncAbortBufferIO(void* buf, bool isForInput);
extern void AsyncTerminateBufferIOByVacuum(void* buffer);
extern void AsyncAbortBufferIOByVacuum(void* buffer);
extern void AbortBufferIO(void);
extern void AbortBufferIO_common(BufferDesc* buf, bool isForInput);
extern void AbortAsyncListIO(void);
extern void CheckIOState(volatile void* bufHdr);
extern void BufmgrCommit(void);
extern bool BgBufferSync(struct WritebackContext* wb_context);
extern XLogRecPtr BufferGetLSNAtomic(Buffer buffer);
extern void AtProcExit_Buffers(int code, Datum arg);
extern void AtProcExit_LocalBuffers(void);

/* in freelist.c */
extern BufferAccessStrategy GetAccessStrategy(BufferAccessStrategyType btype);
extern void FreeAccessStrategy(BufferAccessStrategy strategy);

/* dirty page manager */
extern int ckpt_buforder_comparator(const void* pa, const void* pb);
extern void clean_buf_need_flush_flag(BufferDesc *buf_desc);
extern void ckpt_flush_dirty_page(int thread_id, WritebackContext wb_context);

extern uint32 SyncOneBuffer(
    int buf_id, bool skip_recently_used, WritebackContext* flush_context, bool get_candition_lock = false);

extern Buffer ReadBuffer_common_for_direct(RelFileNode rnode, char relpersistence, ForkNumber forkNum,
    BlockNumber blockNum, ReadBufferMode mode);
extern Buffer ReadBuffer_common_for_localbuf(RelFileNode rnode, char relpersistence, ForkNumber forkNum,
    BlockNumber blockNum, ReadBufferMode mode, BufferAccessStrategy strategy, bool *hit);
extern void DropRelFileNodeShareBuffers(RelFileNode node, ForkNumber forkNum, BlockNumber firstDelBlock);
extern int GetThreadBufferLeakNum(void);

#endif
