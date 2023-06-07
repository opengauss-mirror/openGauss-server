/* ---------------------------------------------------------------------------------------
 * 
 * parallel_reorderbuffer.h
 *        openGauss parallel decoding reorder buffer management.
 * 
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * IDENTIFICATION
 *        src/include/replication/parallel_reorderbuffer.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_REORDERBUFFER_H
#define PARALLEL_REORDERBUFFER_H

#include "lib/ilist.h"

#include "storage/sinval.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/snapshot.h"
#include "utils/timestamp.h"

typedef struct ParallelReorderBuffer ParallelReorderBuffer;
typedef struct ParallelReorderBufferTXN ParallelReorderBufferTXN;
typedef struct ParallelReorderBufferChange ParallelReorderBufferChange;
typedef struct ParallelDecodeReaderWorker ParallelDecodeReaderWorker;

#ifndef LOGICAL_LOG
#define LOGICAL_LOG
enum LogicalLogType {
    LOGICAL_LOG_DML,
    LOGICAL_LOG_COMMIT,
    LOGICAL_LOG_ABORT,
    LOGICAL_LOG_EMPTY,
    LOGICAL_LOG_RUNNING_XACTS,
    LOGICAL_LOG_CONFIRM_FLUSH,
    LOGICAL_LOG_NEW_CID,
    LOGICAL_LOG_MISSING_CHUNK,
    LOGICAL_LOG_ASSIGNMENT
};

typedef struct logicalLog {
    StringInfo out;
    XLogRecPtr lsn;
    XLogRecPtr finalLsn;
    XLogRecPtr endLsn;
    TransactionId xid;
    TransactionId oldestXmin;
    dlist_node node;
    LogicalLogType type;
    CommitSeqNo csn;
    int nsubxacts;
    TransactionId *subXids;
    TimestampTz commitTime;
    RepOriginId origin_id;
    HTAB* toast_hash;
    ParallelReorderBufferTXN *txn;
    logicalLog *freeNext;
} logicalLog;
#endif

ParallelReorderBufferTXN *ParallelReorderBufferTXNByXid(ParallelReorderBuffer *rb, TransactionId xid, bool create, bool *is_new,
                                               XLogRecPtr lsn, bool create_as_top);
typedef void (*ParallelReorderBufferApplyChangeCB)(
    ParallelReorderBuffer* rb, ReorderBufferTXN* txn, Relation relation, ParallelReorderBufferChange* change);

enum ParallelReorderBufferChangeType {
    PARALLEL_REORDER_BUFFER_CHANGE_INSERT,
    PARALLEL_REORDER_BUFFER_CHANGE_UPDATE,
    PARALLEL_REORDER_BUFFER_CHANGE_DELETE,
    PARALLEL_REORDER_BUFFER_CHANGE_RUNNING_XACT,
    PARALLEL_REORDER_BUFFER_CHANGE_COMMIT,
    PARALLEL_REORDER_BUFFER_CHANGE_ABORT,
    PARALLEL_REORDER_BUFFER_CHANGE_UINSERT,
    PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE,
    PARALLEL_REORDER_BUFFER_CHANGE_UDELETE,
    PARALLEL_REORDER_BUFFER_INVALIDATIONS_MESSAGE,
    PARALLEL_REORDER_BUFFER_CHANGE_CONFIRM_FLUSH,
    PARALLEL_REORDER_BUFFER_NEW_CID,
    PARALLEL_REORDER_BUFFER_ASSIGNMENT
};


typedef struct ParallelReorderBufferTXNByIdEnt {
    TransactionId xid;
    ParallelReorderBufferTXN *txn;
} ParallelReorderBufferTXNByIdEnt;

typedef struct ParallelReorderBufferIterTXNEntry {
    XLogRecPtr lsn;
    logicalLog *change;
    ParallelReorderBufferTXN *txn;
    int fd;
    XLogSegNo segno;
} ParallelReorderBufferIterTXNEntry;

typedef struct ParallelReorderBufferIterTXNState {
    binaryheap *heap;
    Size nr_txns;
    dlist_head old_change;
    ParallelReorderBufferIterTXNEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ParallelReorderBufferIterTXNState;
/*
 * a single 'change', can be an insert (with one tuple), an update (old, new),
 * or a delete (old).
 *
 * The same struct is also used internally for other purposes but that should
 * never be visible outside reorderbuffer.c.
 */
typedef struct ParallelReorderBufferChange {
    XLogRecPtr lsn;
    XLogRecPtr finalLsn;
    XLogRecPtr endLsn;
    TransactionId xid;
    TransactionId oldestXmin;
    CommitSeqNo csn;
    int ninvalidations;
    SharedInvalidationMessage *invalidations;
    enum ParallelReorderBufferChangeType action;
    HTAB* toast_hash;
    bool missingChunk;
    /*
     * Context data for the change, which part of the union is valid depends
     * on action/action_internal.
     */
    union {
        /* Old, new tuples when action == *_INSERT|UPDATE|DELETE */
        struct {
            /* relation that has been changed */
            RelFileNode relnode;
            /* no previously reassembled toast chunks are necessary anymore */
            bool clear_toast_afterwards;

            /* valid for DELETE || UPDATE */
            ReorderBufferTupleBuf* oldtuple;
            /* valid for INSERT || UPDATE */
            ReorderBufferTupleBuf* newtuple;
            CommitSeqNo snapshotcsn;
        } tp;
        struct {
            char* ddl_opt;
            char* ddl_type;
            char* ddl_name;
        } ddl_msg;
    } data;
    ParallelReorderBufferChange * freeNext;
    int nsubxacts;
    TransactionId * subXids;
    dlist_node node;
    TimestampTz commitTime;
    RepOriginId origin_id;
} ParallelReorderBufferChange;

typedef struct ParallelReorderBufferTXN {
    /*
     * The transactions transaction id, can be a toplevel or sub xid.
     */
    TransactionId xid;
    TransactionId oldestXid;

    /* did the TX have catalog changes */
    bool has_catalog_changes;

    /* Do we know this is a subxact?  Xid of top-level txn if so */
    bool is_known_as_subxact;

    TransactionId toplevel_xid;

    /*
     * LSN of the first data carrying, WAL record with knowledge about this
     * xid. This is allowed to *not* be first record adorned with this xid, if
     * the previous records aren't relevant for logical decoding.
     */
    XLogRecPtr first_lsn;

    /* ----
     * LSN of the record that lead to this xact to be committed or
     * aborted. This can be a
     * * plain commit record
     * * plain commit record, of a parent transaction
     * * prepared transaction commit
     * * plain abort record
     * * prepared transaction abort
     * * error during decoding
     * ----
     */
    XLogRecPtr final_lsn;

    /*
     * LSN pointing to the end of the commit record + 1.
     */
    XLogRecPtr end_lsn;

    /*
     * LSN of the last lsn at which snapshot information reside, so we can
     * restart decoding from there and fully recover this transaction from
     * WAL.
     */
    XLogRecPtr restart_decoding_lsn;

    /* The csn of the transaction */
    CommitSeqNo csn;

    /* origin of the change that caused this transaction */
    RepOriginId origin_id;

    /*
     * Commit time, only known when we read the actual commit record.
     */
    TimestampTz commit_time;

    /*
     * Base snapshot or NULL.
     * The base snapshot is used to decode all changes until either this
     * transaction modifies the catalog, or another catalog-modifying
     * transaction commits.
     */
    Snapshot base_snapshot;
    XLogRecPtr base_snapshot_lsn;
    dlist_node base_snapshot_node; /* link in txns_by_base_snapshot_lsn */

    /*
     * Has this transaction been spilled to disk?  It's not always possible to
     * deduce that fact by comparing nentries with nentries_mem, because
     * e.g. subtransactions of a large transaction might get serialized
     * together with the parent - if they're restored to memory they'd have
     * nentries_mem == nentries.
     */
    bool serialized;

    /*
     * How many ReorderBufferChange's do we have in this txn.
     *
     * Changes in subtransactions are *not* included but tracked separately.
     */
    uint64 nentries;

    /*
     * How many of the above entries are stored in memory in contrast to being
     * spilled to disk.
     */
    uint64 nentries_mem;

    /*
     * List of ReorderBufferChange structs, including new Snapshots and new
     * CommandIds
     */
    dlist_head changes;

    /*
     * List of (relation, ctid) => (cmin, cmax) mappings for catalog tuples.
     * Those are always assigned to the toplevel transaction. (Keep track of
     * #entries to create a hash of the right size)
     */
    dlist_head tuplecids;
    uint64 ntuplecids;

    /*
     * Hash containing (potentially partial) toast entries. NULL if no toast
     * tuples have been found for the current change.
     */
    HTAB* toast_hash;
    bool missingChunk;
    /*
     * On-demand built hash for looking up the above values.
     */
    HTAB* tuplecid_hash;

    /*
     * non-hierarchical list of subtransactions that are *not* aborted. Only
     * used in toplevel transactions.
     */
    dlist_head subtxns;
    uint32 nsubtxns;

    /*
     * Stored cache invalidations. This is not a linked list because we get
     * all the invalidations at once.
     */
    uint32 ninvalidations;
    SharedInvalidationMessage* invalidations;

    /* ---
     * Position in one of three lists:
     * * list of subtransactions if we are *known* to be subxact
     * * list of toplevel xacts (can be a as-yet unknown subxact)
     * * list of preallocated ReorderBufferTXNs
     * ---
     */
    dlist_node node;

    /*
     * Size of current transaction (changes currently in memory, in bytes).
     */
    Size size;
} ParallelReorderBufferTXN;

struct ParallelReorderBuffer {
    /*
     * xid => ReorderBufferTXN lookup table
     */
    HTAB* by_txn;

    /*
     * Transactions that could be a toplevel xact, ordered by LSN of the first
     * record bearing that xid..
     */
    dlist_head toplevel_by_lsn;

    /*
     * Transactions and subtransactions that have a base snapshot, ordered by
     * LSN of the record which caused us to first obtain the base snapshot.
     * This is not the same as toplevel_by_lsn, because we only set the base
     * snapshot on the first logical-decoding-relevant record (eg. heap
     * writes), whereas the initial LSN could be set by other operations.
     */
    dlist_head txns_by_base_snapshot_lsn;

    /*
     * one-entry sized cache for by_txn. Very frequently the same txn gets
     * looked up over and over again.
     */
    TransactionId by_txn_last_xid;
    ParallelReorderBufferTXN* by_txn_last_txn;

    /*
     * Callacks to be called when a transactions commits.
     */
    ReorderBufferBeginCB begin;
    ParallelReorderBufferApplyChangeCB apply_change;
    ReorderBufferCommitCB commit;

    /*
     * Pointer that will be passed untouched to the callbacks.
     */
    void* private_data;

    /*
     * Private memory context.
     */
    MemoryContext context;

    /*
     * Data structure slab cache.
     *
     * We allocate/deallocate some structures very frequently, to avoid bigger
     * overhead we cache some unused ones here.
     *
     * The maximum number of cached entries is controlled by const variables
     * ontop of reorderbuffer.c
     */

    /* cached ParallelReorderBufferTXNs */
    dlist_head cached_transactions;
    Size nr_cached_transactions;

    /* cached ParallelReorderBufferChanges */
    dlist_head cached_changes;
    Size nr_cached_changes;

    /* cached ParallelReorderBufferTupleBufs */
    slist_head cached_tuplebufs;
    Size nr_cached_tuplebufs;
    TransactionId lastRunningXactOldestXmin;

    XLogRecPtr current_restart_decoding_lsn;

    /* buffer for disk<->memory conversions */
    char* outbuf;
    Size outbufsize;

    /* memory accounting */
    Size size;
};

/* Disk serialization support datastructures */
typedef struct ParallelReorderBufferDiskChange {
    Size size;
    logicalLog change;
    /* data follows */
} ParallelReorderBufferDiskChange;

extern ParallelReorderBuffer* ParallelReorderBufferAllocate(int slotId);

extern void ParallelFreeTuple(ReorderBufferTupleBuf *tuple, int slotId);
extern void ParallelFreeChange(ParallelReorderBufferChange *change, int slotId);
extern ParallelReorderBufferChange* ParallelReorderBufferGetChange(ParallelReorderBuffer *rb, int slotId);
extern ReorderBufferTupleBuf *ParallelReorderBufferGetTupleBuf(ParallelReorderBuffer *rb, Size tuple_len,
    ParallelDecodeReaderWorker *worker, bool isHeapTuple);
extern void ParallelReorderBufferToastReset(HTAB** toastHash, int slotId);
extern void WalSndWriteDataHelper(StringInfo out, XLogRecPtr lsn, TransactionId xid, bool last_write);
extern void WalSndPrepareWriteHelper(StringInfo out, XLogRecPtr lsn, TransactionId xid, bool last_write);
extern void ParallelReorderBufferUpdateMemory(ParallelReorderBuffer *rb, logicalLog *change, int slotId, bool add);
extern void CheckNewTupleMissingToastChunk(ParallelReorderBufferChange *change, bool isHeap);
extern void ParallelReorderBufferChildAssignment(ParallelReorderBuffer *prb, logicalLog *logChange);
extern void ParallelReorderBufferCleanupTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn,
    XLogRecPtr lsn = InvalidXLogRecPtr);

const uint32 max_decode_cache_num = 100000;
#endif
