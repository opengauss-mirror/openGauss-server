/* -------------------------------------------------------------------------
 *
 * reorderbuffer.cpp
 *	openGauss logical replay/reorder buffer management
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/reorderbuffer.cpp
 *
 * NOTES
 *	This module gets handed individual pieces of transactions in the order
 *	they are written to the WAL and is responsible to reassemble them into
 *	toplevel transaction sized pieces. When a transaction is completely
 *	reassembled - signalled by reading the transaction commit record - it
 *	will then call the output plugin (c.f. ReorderBufferCommit()) with the
 *	individual changes. The output plugins rely on snapshots built by
 *	snapbuild.c which hands them to us.
 *
 *	Transactions and subtransactions/savepoints in openGauss are not
 *	immediately linked to each other from outside the performing
 *	backend. Only at commit/abort (or special xact_assignment records) they
 *	are linked together. Which means that we will have to splice together a
 *	toplevel transaction from its subtransactions. To do that efficiently we
 *	build a binary heap indexed by the smallest current lsn of the individual
 *	subtransactions' changestreams. As the individual streams are inherently
 *	ordered by LSN - since that is where we build them from - the transaction
 *	can easily be reassembled by always using the subtransaction with the
 *	smallest current LSN from the heap.
 *
 *	In order to cope with large transactions - which can be several times as
 *	big as the available memory - this module supports spooling the contents
 *	of a large transactions to disk. When the transaction is replayed the
 *	contents of individual (sub-)transactions will be read from disk in
 *	chunks.
 *
 *	This module also has to deal with reassembling toast records from the
 *	individual chunks stored in WAL. When a new (or initial) version of a
 *	tuple is stored in WAL it will always be preceded by the toast chunks
 *	emitted for the columns stored out of line. Within a single toplevel
 *	transaction there will be no other data carrying records between a row's
 *	toast chunks and the row data itself. See ReorderBufferToast* for
 *	details.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "miscadmin.h"

#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catalog.h"
#include "catalog/gs_matview.h"
#include "catalog/pg_namespace.h"
#include "lib/binaryheap.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/snapbuild.h" /* just for SnapBuildSnapDecRefcount */
#include "access/xlog_internal.h"
#include "replication/ddlmessage.h"

#include "storage/buf/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/relfilenode_hash.h"
#include "storage/sinval.h"

#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "storage/file/fio_device.h"

/*
 * We use a very simple form of a slab allocator for frequently allocated
 * objects, simply keeping a fixed number in a linked list when unused,
 * instead pfree()ing them. Without that in many workloads aset.c becomes a
 * major bottleneck, especially when spilling to disk while decoding batch
 * workloads.
 */
static const Size max_cached_changes = 4096 * 2;
static const Size g_max_cached_transactions = 512;

static const Size sizeMB = 1024L * 1024L;
static const Size sizeGB = 1024L * 1024L * 1024L;

#define MaxReorderBufferTupleSize (Max(MaxHeapTupleSize, MaxPossibleUHeapTupleSize))
/* ---------------------------------------
 * primary reorderbuffer support routines
 * ---------------------------------------
 */
static ReorderBufferTXN *ReorderBufferGetTXN(ReorderBuffer *rb);
static void ReorderBufferReturnTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);
ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid, bool create, bool *is_new,
                                               XLogRecPtr lsn, bool create_as_top);

static void AssertTXNLsnOrder(ReorderBuffer *rb);
static void ReorderBufferTransferSnapToParent(ReorderBufferTXN *txn, ReorderBufferTXN *subtxn);

/* ---------------------------------------
 * support functions for lsn-order iterating over the ->changes of a
 * transaction and its subtransactions
 *
 * used for iteration over the k-way heap merge of a transaction and its
 * subtransactions
 * ---------------------------------------
 */
static ReorderBufferIterTXNState *ReorderBufferIterTXNInit(ReorderBuffer *rb, ReorderBufferTXN *txn);
static ReorderBufferChange *ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state);
static void ReorderBufferIterTXNFinish(ReorderBuffer *rb, ReorderBufferIterTXNState *state);
static void ReorderBufferExecuteInvalidations(uint32 nmsgs, SharedInvalidationMessage *msgs);

/*
 * ---------------------------------------
 * Disk serialization support functions
 * ---------------------------------------
 */
static void ReorderBufferProcessTXN(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr commit_lsn, 
    volatile Snapshot snapshot_now, volatile CommandId command_id, bool streaming);
static void ReorderBufferCheckSerializeTXN(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, ReorderBufferChange *change, bool toast_insert);
static void ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn, int fd, ReorderBufferChange *change);
static int ReorderBufferTXNSizeCompare(const pairingheap_node *a, const pairingheap_node *b, void *arg);
static Size ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn, int *fd, XLogSegNo *segno);
static void ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn, char *change);
static void ReorderBufferRestoreCleanup(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr lsn);
static void ReorderBufferMaybeMarkTXNStreamed(ReorderBuffer *rb, ReorderBufferTXN *txn);
static bool ReorderBufferCheckAndTruncateAbortedTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);

static void ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap);
static Snapshot ReorderBufferCopySnap(ReorderBuffer *rb, Snapshot orig_snap, ReorderBufferTXN *txn, CommandId cid);
static void ReorderBufferReturnHeapChange(ReorderBuffer *rb, ReorderBufferChange *change);
static void ReorderBufferReturnDDLChange(ReorderBufferChange *change);
static void ReorderBufferReturnSnapshotChange(ReorderBuffer *rb, ReorderBufferChange *change);
static void ReorderBufferReturnUHeapChange(ReorderBuffer *rb, ReorderBufferChange *change);

/* ---------------------------------------
 * toast reassembly support
 * ---------------------------------------
 */
static void ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferToastReset(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferToastReplace(ReorderBuffer *rb, ReorderBufferTXN *txn, Relation relation,
                                      ReorderBufferChange *change, Oid partationReltoastrelid, bool isUHeap);
static void ReorderBufferToastAppendChunk(ReorderBuffer *rb, ReorderBufferTXN *txn, Relation relation,
                                          ReorderBufferChange *change, bool isUHeap);

/*
 * ---------------------------------------
 * memory accounting
 * ---------------------------------------
 */
static Size ReorderBufferChangeSize(ReorderBufferChange *change);
static void ReorderBufferChangeMemoryUpdate(ReorderBuffer *rb, ReorderBufferChange *change,
                                            ReorderBufferTXN *txn, bool addition, Size sz);

/*
 * Allocate a new ReorderBuffer
 */
ReorderBuffer *ReorderBufferAllocate(void)
{
    ReorderBuffer *buffer = NULL;
    HASHCTL hash_ctl;
    MemoryContext new_ctx = NULL;
    int rc = 0;
    /* allocate memory in own context, to have better accountability */
    new_ctx = AllocSetContextCreate(CurrentMemoryContext, "ReorderBuffer", ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    buffer = (ReorderBuffer *)MemoryContextAlloc(new_ctx, sizeof(ReorderBuffer));

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");

    buffer->context = new_ctx;

    hash_ctl.keysize = sizeof(TransactionId);
    hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = buffer->context;

    buffer->by_txn = hash_create("ReorderBufferByXid", 1000, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    buffer->by_txn_last_xid = InvalidTransactionId;
    buffer->by_txn_last_txn = NULL;

    buffer->nr_cached_transactions = 0;
    buffer->nr_cached_changes = 0;
    buffer->nr_cached_tuplebufs = 0;

    buffer->outbuf = NULL;
    buffer->outbufsize = 0;
    buffer->size = 0;

    /* txn_heap is ordered by transaction size */
    buffer->txn_heap = pairingheap_allocate(ReorderBufferTXNSizeCompare, NULL);

    buffer->spillTxns = 0;
    buffer->spillCount = 0;
    buffer->spillBytes = 0;
    buffer->streamTxns = 0;
    buffer->streamCount = 0;
    buffer->streamBytes = 0;
    buffer->totalTxns = 0;
    buffer->totalBytes = 0;

    buffer->current_restart_decoding_lsn = InvalidXLogRecPtr;

    dlist_init(&buffer->toplevel_by_lsn);
    dlist_init(&buffer->txns_by_base_snapshot_lsn);
    dlist_init(&buffer->cached_transactions);
    dlist_init(&buffer->cached_changes);
    slist_init(&buffer->cached_tuplebufs);

    if (t_thrd.slot_cxt.MyReplicationSlot != NULL) {
        ReorderBufferClear(NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));
    }
    return buffer;
}

/*
 * Free a ReorderBuffer
 */
void ReorderBufferFree(ReorderBuffer *rb)
{
    MemoryContext context = rb->context;

    /*
     * We free separately allocated data by entirely scrapping reorderbuffer's
     * memory context.
     */
    MemoryContextDelete(context);
    if (t_thrd.slot_cxt.MyReplicationSlot != NULL) {
        ReorderBufferClear(NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));
    }
}

/*
 * Get a unused, possibly preallocated, ReorderBufferTXN.
 */
static ReorderBufferTXN *ReorderBufferGetTXN(ReorderBuffer *rb)
{
    ReorderBufferTXN *txn = NULL;
    int rc = 0;
    /* check the slab cache */
    if (rb->nr_cached_transactions > 0) {
        rb->nr_cached_transactions--;
        txn = (ReorderBufferTXN *)dlist_container(ReorderBufferTXN, node,
                                                  dlist_pop_head_node(&rb->cached_transactions));
    } else {
        txn = (ReorderBufferTXN *)MemoryContextAlloc(rb->context, sizeof(ReorderBufferTXN));
    }

    rc = memset_s(txn, sizeof(ReorderBufferTXN), 0, sizeof(ReorderBufferTXN));
    securec_check(rc, "", "");

    dlist_init(&txn->changes);
    dlist_init(&txn->tuplecids);
    dlist_init(&txn->subtxns);

    txn->output_plugin_private = NULL;
    /* InvalidCommandId is not zero, so set it explicitly */
    txn->command_id = InvalidCommandId;

    return txn;
}

/*
 * Free a ReorderBufferTXN.
 *
 * Deallocation might be delayed for efficiency purposes, for details check
 * the comments above max_cached_changes's definition.
 */
void ReorderBufferReturnTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    /* clean the lookup cache if we were cached (quite likely) */
    if (rb->by_txn_last_xid == txn->xid) {
        rb->by_txn_last_xid = InvalidTransactionId;
        rb->by_txn_last_txn = NULL;
    }

    /* free data that's contained */
    if (txn->tuplecid_hash != NULL) {
        hash_destroy(txn->tuplecid_hash);
        txn->tuplecid_hash = NULL;
    }

    if (txn->invalidations) {
        pfree(txn->invalidations);
        txn->invalidations = NULL;
    }

    /* All changes must be deallocated */
    Assert(txn->size == 0);

    /* check whether to put into the slab cache */
    if (rb->nr_cached_transactions < g_max_cached_transactions) {
        rb->nr_cached_transactions++;
        dlist_push_head(&rb->cached_transactions, &txn->node);
    } else {
        pfree(txn);
        txn = NULL;
    }
}

/*
 * Get a unused, possibly preallocated, ReorderBufferChange.
 */
ReorderBufferChange *ReorderBufferGetChange(ReorderBuffer *rb)
{
    ReorderBufferChange *change = NULL;
    int rc = 0;
    /* check the slab cache */
    if (rb->nr_cached_changes) {
        rb->nr_cached_changes--;
        change = (ReorderBufferChange *)dlist_container(ReorderBufferChange, node,
                                                        dlist_pop_head_node(&rb->cached_changes));
    } else {
        change = (ReorderBufferChange *)MemoryContextAlloc(rb->context, sizeof(ReorderBufferChange));
    }

    rc = memset_s(change, sizeof(ReorderBufferChange), 0, sizeof(ReorderBufferChange));
    securec_check(rc, "", "");

    return change;
}

/*
 * Calculate size of a change in memory.
 */
static Size ReorderBufferChangeSize(ReorderBufferChange *change)
{
    Size sz = sizeof(ReorderBufferChange);
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE: {
            if (change->data.tp.newtuple) {
                sz += sizeof(HeapTupleData) + change->data.tp.newtuple->tuple.t_len;
            }

            if (change->data.tp.oldtuple) {
                sz += sizeof(HeapTupleData) + change->data.tp.oldtuple->tuple.t_len;
            }
#ifdef ENABLE_NEON
            if (change->data.tp.changed_attrs_valid) {
                sz += sizeof(AttrNumber) * change->data.tp.nchanged_attrs;
            }
#endif
            break;
        }
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT: {
            Snapshot snap = change->data.snapshot;
            sz += sizeof(SnapshotData) + sizeof(TransactionId) * snap->xcnt + sizeof(TransactionId) * snap->subxcnt;
            break;
        }
        case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
        case REORDER_BUFFER_CHANGE_UUPDATE:
        case REORDER_BUFFER_CHANGE_UDELETE: {
            if (change->data.utp.newtuple) {
                sz += sizeof(UHeapTupleData) + change->data.utp.newtuple->tuple.disk_tuple_size;
            }

            if (change->data.utp.oldtuple) {
                sz += sizeof(UHeapTupleData) + change->data.utp.oldtuple->tuple.disk_tuple_size;
            }
            break;
        }
        case REORDER_BUFFER_CHANGE_DDL: {
            Size prefix_size = strlen(change->data.ddl.prefix) + 1;

            sz += prefix_size + change->data.ddl.message_size + sizeof(Size) + sizeof(Size) + sizeof(Oid) + sizeof(DeparsedCommandType);

            break;
        }
        case REORDER_BUFFER_CHANGE_TRUNCATE:
            break;
        case REORDER_BUFFER_CHANGE_INVALIDATION: {
            sz += sizeof(SharedInvalidationMessage) * change->data.inval.ninvalidations;
            break;
        }
    }
    return sz;
}

/*
 * Update memory counters to account for the new or removed change.
 *
 * We update two counters - in the reorder buffer, and in the transaction
 * containing the change. The reorder buffer counter allows us to quickly
 * decide if we reached the memory limit, the transaction counter allows
 * us to quickly pick the largest transaction for eviction.
 *
 * Either txn or change must be non-NULL at least. We update the memory
 * counter of txn if it's non-NULL, otherwise change->txn.
 *
 * When streaming is enabled, we need to update the toplevel transaction
 * counters instead - we don't really care about subtransactions as we
 * can't stream them individually anyway, and we only pick toplevel
 * transactions for eviction. So only toplevel transactions matter.
 */
static void ReorderBufferChangeMemoryUpdate(ReorderBuffer *rb, ReorderBufferChange *change,
    ReorderBufferTXN *txn, bool addition, Size sz)
{
    ReorderBufferTXN *toptxn;
    Assert(txn || change);

    /*
     * Ignore tuple CID changes, because those are not evicted when reaching
     * memory limit. So we just don't count them, because it might easily
     * trigger a pointless attempt to spill.
     */
    if (change && change->action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID)
        return;
    if (sz == 0)
        return;

    if (txn == NULL)
        txn = change->txn;
    Assert(txn != NULL);

    /*
     * Update the total size in top level as well. This is later used to
     * compute the decoding stats.
     */
    toptxn = rbtxn_get_toptxn(txn);

    if (addition) {
        Size oldsize = txn->size;
        txn->size += sz;
        rb->size += sz;
        /* Update the total size in the top transaction. */
        toptxn->total_size += sz;

        /* Update the max-heap */
        if (oldsize != 0)
            pairingheap_remove(rb->txn_heap, &txn->txn_node);
        pairingheap_add(rb->txn_heap, &txn->txn_node);
    } else {
        Assert((rb->size >= sz) && (txn->size >= sz));
        txn->size -= sz;
        rb->size -= sz;
        /* Update the total size in the top transaction. */
        toptxn->total_size -= sz;

        /* Update the max-heap */
        pairingheap_remove(rb->txn_heap, &txn->txn_node);
        if (txn->size != 0)
            pairingheap_add(rb->txn_heap, &txn->txn_node);
    }

    Assert(txn->size <= rb->size);
}

/*
 * Free an ReorderBufferChange.
 *
 * Deallocation might be delayed for efficiency purposes, for details check
 * the comments above max_cached_changes's definition.
 */
void ReorderBufferReturnChange(ReorderBuffer *rb, ReorderBufferChange *change, bool upd_mem)
{
    /* update memory accounting info */
    if (upd_mem) {
        ReorderBufferChangeMemoryUpdate(rb, change, NULL, false, ReorderBufferChangeSize(change));
    }

    /* free contained data */
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
            ReorderBufferReturnHeapChange(rb, change);
            break;
        case REORDER_BUFFER_CHANGE_DDL:
            ReorderBufferReturnDDLChange(change);
            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
            ReorderBufferReturnSnapshotChange(rb, change);
            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
        case REORDER_BUFFER_CHANGE_UUPDATE:
        case REORDER_BUFFER_CHANGE_UDELETE:
            ReorderBufferReturnUHeapChange(rb, change);
            break;
        case REORDER_BUFFER_CHANGE_TRUNCATE:
            break;
        case REORDER_BUFFER_CHANGE_INVALIDATION: {
            if (change->data.inval.invalidations)
                pfree(change->data.inval.invalidations);
            change->data.inval.invalidations = NULL;
            break;
        }
    }

    pfree(change);
    change = NULL;
}

static void ReorderBufferReturnHeapChange(ReorderBuffer *rb, ReorderBufferChange *change)
{
    if (change->data.tp.newtuple) {
        ReorderBufferReturnTupleBuf(rb, change->data.tp.newtuple);
        change->data.tp.newtuple = NULL;
    }

    if (change->data.tp.oldtuple) {
        ReorderBufferReturnTupleBuf(rb, change->data.tp.oldtuple);
        change->data.tp.oldtuple = NULL;
    }
#ifdef ENABLE_NEON
    if (change->data.tp.changed_attrs != NULL) {
        pfree(change->data.tp.changed_attrs);
        change->data.tp.changed_attrs = NULL;
    }
#endif
}

static void ReorderBufferReturnDDLChange(ReorderBufferChange *change)
{
    if (change->data.ddl.prefix != NULL)
        pfree(change->data.ddl.prefix);
    change->data.ddl.prefix = NULL;
    if (change->data.ddl.message != NULL)
        pfree(change->data.ddl.message);
    change->data.ddl.message = NULL;
}

static void ReorderBufferReturnSnapshotChange(ReorderBuffer *rb, ReorderBufferChange *change)
{
    if (change->data.snapshot) {
        ReorderBufferFreeSnap(rb, change->data.snapshot);
        change->data.snapshot = NULL;
    }
}

static void ReorderBufferReturnUHeapChange(ReorderBuffer *rb, ReorderBufferChange *change)
{
    if (change->data.utp.newtuple) {
        ReorderBufferReturnTupleBuf(rb, (ReorderBufferTupleBuf*)change->data.utp.newtuple);
        change->data.utp.newtuple = NULL;
    }

    if (change->data.utp.oldtuple) {
        ReorderBufferReturnTupleBuf(rb, (ReorderBufferTupleBuf*)change->data.utp.oldtuple);
        change->data.utp.oldtuple = NULL;
    }
}

/*
 * Get an unused, possibly preallocated, ReorderBufferTupleBuf fitting at
 * least a tuple of size tuple_len (excluding header overhead).
 */
ReorderBufferTupleBuf *ReorderBufferGetTupleBuf(ReorderBuffer *rb, Size tuple_len)
{
    ReorderBufferTupleBuf *tuple = NULL;
    Size alloc_len = tuple_len + SizeofHeapTupleHeader;
    /*
     * Most tuples are below MaxReorderBufferTupleSize, so we use a slab allocator for
     * those. Thus always allocate at least MaxReorderBufferTupleSize. Note that tuples
     * tuples generated for oldtuples can be bigger, as they don't have
     * out-of-line toast columns.
     */
    if (alloc_len < MaxReorderBufferTupleSize)
        alloc_len = MaxReorderBufferTupleSize;

    /* if small enough, check the slab cache */
    if (alloc_len <= MaxReorderBufferTupleSize && rb->nr_cached_tuplebufs) {
        rb->nr_cached_tuplebufs--;
        tuple = slist_container(ReorderBufferTupleBuf, node, slist_pop_head_node(&rb->cached_tuplebufs));
#ifdef USE_ASSERT_CHECKING
        int rc = memset_s(&tuple->tuple, sizeof(HeapTupleData), 0xa9, sizeof(HeapTupleData));
        securec_check(rc, "", "");

#endif
        tuple->tuple.t_data = ReorderBufferTupleBufData(tuple);
#ifdef USE_ASSERT_CHECKING
        int ret = memset_s(tuple->tuple.t_data, tuple->alloc_tuple_size, 0xa8, tuple->alloc_tuple_size);
        securec_check(ret, "", "");

#endif
    } else {
        tuple = (ReorderBufferTupleBuf *)MemoryContextAlloc(rb->context, sizeof(ReorderBufferTupleBuf) + alloc_len);
        tuple->alloc_tuple_size = alloc_len;
        tuple->tuple.t_data = ReorderBufferTupleBufData(tuple);
    }
    tuple->tuple.tupTableType = HEAP_TUPLE;
    return tuple;
}

/*
 * Get an unused, possibly preallocated, ReorderBufferUTupleBuf fitting at
 * least a tuple of size tupleLen (excluding header overhead).
 */
ReorderBufferUTupleBuf *ReorderBufferGetUTupleBuf(ReorderBuffer *rb, Size tupleLen)
{
    ReorderBufferUTupleBuf *tuple = NULL;
    Size allocLen = add_size(tupleLen, SizeOfUHeapDiskTupleData);
    /*
     * Most tuples are below MaxReorderBufferTupleSize, so we use a slab allocator for
     * those. Thus always allocate at least MaxReorderBufferTupleSize. Note that tuples
     * tuples generated for oldtuples can be bigger, as they don't have
     * out-of-line toast columns.
     */
    if (allocLen < MaxReorderBufferTupleSize)
        allocLen = MaxReorderBufferTupleSize;

    /* if small enough, check the slab cache */
    if (allocLen <= MaxReorderBufferTupleSize && rb->nr_cached_tuplebufs) {
        rb->nr_cached_tuplebufs--;
        tuple = slist_container(ReorderBufferUTupleBuf, node, slist_pop_head_node(&rb->cached_tuplebufs));
#ifdef USE_ASSERT_CHECKING
        int rc = memset_s(&tuple->tuple, sizeof(UHeapTupleData), 0xa9, sizeof(UHeapTupleData));
        securec_check(rc, "", "");

#endif
        tuple->tuple.disk_tuple = ReorderBufferUTupleBufData(tuple);
#ifdef USE_ASSERT_CHECKING
        int ret = memset_s(tuple->tuple.disk_tuple, tuple->alloc_tuple_size, 0xa8, tuple->alloc_tuple_size);
        securec_check(ret, "", "");

#endif
    } else {
        tuple = (ReorderBufferUTupleBuf *)MemoryContextAlloc(rb->context, sizeof(ReorderBufferUTupleBuf) + allocLen);
        tuple->alloc_tuple_size = allocLen;
        tuple->tuple.disk_tuple = ReorderBufferUTupleBufData(tuple);
    }
    tuple->tuple.tupTableType = UHEAP_TUPLE;
    return tuple;
}

/*
 * Free an ReorderBufferTupleBuf.
 *
 * Deallocation might be delayed for efficiency purposes, for details check
 * the comments above max_cached_changes's definition.
 */
void ReorderBufferReturnTupleBuf(ReorderBuffer *rb, ReorderBufferTupleBuf *tuple)
{
    /* check whether to put into the slab cache, oversized tuples never are */
    if (tuple->alloc_tuple_size == MaxReorderBufferTupleSize &&
        rb->nr_cached_tuplebufs < (unsigned)g_instance.attr.attr_common.max_cached_tuplebufs) {
        rb->nr_cached_tuplebufs++;
        slist_push_head(&rb->cached_tuplebufs, &tuple->node);
    } else {
        pfree(tuple);
        tuple = NULL;
    }
}

/*
 * Return the ReorderBufferTXN from the given buffer, specified by Xid.
 * If create is true, and a transaction doesn't already exist, create it
 * (with the given LSN, and as top transaction if that's specified);
 * when this happens, is_new is set to true.
 */
ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid, bool create, bool *is_new,
                                               XLogRecPtr lsn, bool create_as_top)
{
    ReorderBufferTXN *txn = NULL;
    ReorderBufferTXNByIdEnt *ent = NULL;
    bool found = false;

    Assert(TransactionIdIsValid(xid));

    /*
     * Check the one-entry lookup cache first
     */
    if (TransactionIdIsValid(rb->by_txn_last_xid) && rb->by_txn_last_xid == xid) {
        txn = rb->by_txn_last_txn;

        if (txn != NULL) {
            /* found it, and it's valid */
            if (is_new != NULL) {
                *is_new = false;
            }
            return txn;
        }

        /*
         * cached as non-existant, and asked not to create? Then nothing else
         * to do.
         */
        if (!create) {
            return NULL;
        }
        /* otherwise fall through to create it */
    }

    /*
     * If the cache wasn't hit or it yielded an "does-not-exist" and we want
     * to create an entry.
     *
     * search the lookup table
     */
    ent = (ReorderBufferTXNByIdEnt *)hash_search(rb->by_txn, (void *)&xid, create ? HASH_ENTER : HASH_FIND, &found);
    if (found) {
        txn = ent->txn;
    } else if (create) {
        /* initialize the new entry, if creation was requested */
        Assert(ent != NULL);
        Assert(lsn != InvalidXLogRecPtr);

        ent->txn = ReorderBufferGetTXN(rb);
        ent->txn->xid = xid;
        txn = ent->txn;
        txn->first_lsn = lsn;
        txn->restart_decoding_lsn = rb->current_restart_decoding_lsn;

        if (create_as_top) {
            dlist_push_tail(&rb->toplevel_by_lsn, &txn->node);
            AssertTXNLsnOrder(rb);
        }
    } else {
        txn = NULL; /* not found and not asked to create */
    }

    /* update cache */
    rb->by_txn_last_xid = xid;
    rb->by_txn_last_txn = txn;

    if (is_new != NULL) {
        *is_new = !found;
    }

    Assert(!create || !!txn);
    return txn;
}

/*
 * Queue a change into a transaction so it can be replayed upon commit.
 */
void ReorderBufferQueueChange(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn,
    ReorderBufferChange *change, bool toast_insert)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(ctx->reorder, xid, true, NULL, lsn, true);

    /*
     * If we have detected that the transaction is aborted while streaming the
     * previous changes or by checking its CLOG, there is no point in
     * collecting further changes for it.
     */
    if (rbtxn_is_aborted(txn)) {
        /*
         * We don't need to update memory accounting for this change as we
         * have not added it to the queue yet.
         */
        ReorderBufferReturnChange(ctx->reorder, change, false);
        return;
    }

    /*
     * The changes that are sent downstream are considered streamable.  We
     * remember such transactions so that only those will later be considered
     * for streaming.
     */
    if (change->action == REORDER_BUFFER_CHANGE_INSERT ||
        change->action == REORDER_BUFFER_CHANGE_UPDATE ||
        change->action == REORDER_BUFFER_CHANGE_DELETE ||
        change->action == REORDER_BUFFER_CHANGE_TRUNCATE ||
        change->action == REORDER_BUFFER_CHANGE_UINSERT ||
        change->action == REORDER_BUFFER_CHANGE_UUPDATE ||
        change->action == REORDER_BUFFER_CHANGE_UDELETE)
    {
        ReorderBufferTXN *toptxn = rbtxn_get_toptxn(txn);

        toptxn->txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE;
    }

    change->lsn = lsn;
    change->txn = txn;
    Assert(!XLByteEQ(InvalidXLogRecPtr, lsn));
    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries++;
    txn->nentries_mem++;

    /* update memory accounting information */
    ReorderBufferChangeMemoryUpdate(ctx->reorder, change, NULL, true, ReorderBufferChangeSize(change));
    ReorderBufferCheckSerializeTXN(ctx, txn, change, toast_insert);
}

/*
 * Special handling specifically for XLH_DELETE_IS_SUPER
 *
 * XLH_DELETE_IS_SUPER records are generated during the execution of an UPSERT statement.
 * Specifically, after performing an INSERT, a conflict is detected with a concurrently
 * committed transaction, leading to a DELETE and a UPDATE.
 *
 * Such a UPSERT with INSERT, DELETE and UPDATE cannot be logical decoded normally, because
 * it would be unexecutable while INSERT might cause a key-conflict error. So when we meet
 * such a delete record, we remove the last INSERT change and don't add DELETE change.
 */
void ReorderBufferRemoveChangeForUpsert(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(ctx->reorder, xid, true, NULL, lsn, true);

    /* there must be at least one entry */
    Assert(txn->nentries > 0);

    dlist_node *node = dlist_tail_node(&txn->changes);
    ReorderBufferChange* entry = dlist_container(ReorderBufferChange, node, node);

    /* last entry must be INSERT */
    Assert(entry->action == REORDER_BUFFER_CHANGE_INSERT);

    dlist_delete(node);
    txn->nentries--;
    txn->nentries_mem--;
    pfree(entry);
}

/*
 * A transactional DDL message is queued to be processed upon commit.
 */
void
ReorderBufferQueueDDLMessage(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn,
    const char*prefix, Size message_size, const char *message, Oid relid, DeparsedCommandType cmdtype)
{
    int rc = 0;
    ReorderBuffer *rb = ctx->reorder;
    MemoryContext oldcontext;
    ReorderBufferChange *change;

    Assert(TransactionIdIsValid(xid));

    oldcontext = MemoryContextSwitchTo(rb->context);

    change = ReorderBufferGetChange(rb);
    change->action = REORDER_BUFFER_CHANGE_DDL;
    change->data.ddl.prefix = pstrdup(prefix);
    change->data.ddl.relid = relid;
    change->data.ddl.cmdtype = cmdtype;
    change->data.ddl.message_size = message_size;
    change->data.ddl.message = (char*)palloc(message_size);
    rc = memcpy_s(change->data.ddl.message, message_size, message, message_size);
    securec_check(rc, "", "");

    ReorderBufferQueueChange(ctx, xid, lsn, change);
    MemoryContextSwitchTo(oldcontext);
}
/*
 * AssertTXNLsnOrder
 *     Verify LSN ordering of transaction lists in the reorderbuffer
 *
 * Other LSN-related invariants are checked too.
 *
 * No-op if assertions are not in use.
 */
static void AssertTXNLsnOrder(ReorderBuffer *rb)
{
#ifdef USE_ASSERT_CHECKING
    dlist_iter iter;
    XLogRecPtr prev_first_lsn = InvalidXLogRecPtr;
    XLogRecPtr prev_base_snap_lsn = InvalidXLogRecPtr;

    dlist_foreach(iter, &rb->toplevel_by_lsn)
    {
        ReorderBufferTXN *cur_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
        /* start LSN must be set */
        Assert(!XLByteEQ(cur_txn->first_lsn, InvalidXLogRecPtr));

        /* If there is an end LSN, it must be higher than start LSN */
        if (!XLByteEQ(cur_txn->end_lsn, InvalidXLogRecPtr))
            Assert(XLByteLE(cur_txn->first_lsn, cur_txn->end_lsn));

        /* Current initial LSN must be strictly higher than previous */
        if (!XLByteEQ(prev_first_lsn, InvalidXLogRecPtr))
            Assert(XLByteLE(prev_first_lsn, cur_txn->first_lsn));

        /* known-as-subtxn txns must not be listed */
        Assert(!rbtxn_is_known_subxact(cur_txn));
        prev_first_lsn = cur_txn->first_lsn;
    }
    dlist_foreach(iter, &rb->txns_by_base_snapshot_lsn)
    {
        ReorderBufferTXN *cur_txn = dlist_container(ReorderBufferTXN, base_snapshot_node, iter.cur);

        /* base snapshot (and its LSN) must be set */
        Assert(cur_txn->base_snapshot != NULL);
        Assert(cur_txn->base_snapshot_lsn != InvalidXLogRecPtr);

        /* current LSN must be strictly higher than previous */
        if (prev_base_snap_lsn != InvalidXLogRecPtr)
            Assert(prev_base_snap_lsn < cur_txn->base_snapshot_lsn);

        /* known-as-subtxn txns must not be listed */
        Assert(!rbtxn_is_known_subxact(cur_txn));

        prev_base_snap_lsn = cur_txn->base_snapshot_lsn;
    }

#endif
}

/*
 * ReorderBufferGetOldestTXN
 *     Return oldest transaction in reorderbuffer
 */
ReorderBufferTXN *ReorderBufferGetOldestTXN(ReorderBuffer *rb)
{
    ReorderBufferTXN *txn = NULL;

    AssertTXNLsnOrder(rb);

    if (dlist_is_empty(&rb->toplevel_by_lsn))
        return NULL;

    txn = dlist_head_element(ReorderBufferTXN, node, &rb->toplevel_by_lsn);

    Assert(!rbtxn_is_known_subxact(txn));
    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));
    return txn;
}

/*
 * ReorderBufferGetOldestXmin
 *     Return oldest Xmin in reorderbuffer
 *
 * Returns oldest possibly running Xid from the point of view of snapshots
 * used in the transactions kept by reorderbuffer, or InvalidTransactionId if
 * there are none.
 *
 * Since snapshots are assigned monotonically, this equals the Xmin of the
 * base snapshot with minimal base_snapshot_lsn.
 */
TransactionId ReorderBufferGetOldestXmin(ReorderBuffer *rb)
{
    ReorderBufferTXN *txn = NULL;

    AssertTXNLsnOrder(rb);

    if (dlist_is_empty(&rb->txns_by_base_snapshot_lsn))
        return InvalidTransactionId;

    txn = dlist_head_element(ReorderBufferTXN, base_snapshot_node, &rb->txns_by_base_snapshot_lsn);
    return txn->base_snapshot->xmin;
}

void ReorderBufferSetRestartPoint(ReorderBuffer *rb, XLogRecPtr ptr)
{
    rb->current_restart_decoding_lsn = ptr;
}

/* Make note that we know that subxid is a subtransaction of xid, seen as of the given lsn. */
void ReorderBufferAssignChild(ReorderBuffer *rb, TransactionId xid, TransactionId subxid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn = NULL;
    ReorderBufferTXN *subtxn = NULL;
    bool new_top = false;
    bool new_sub = false;

    if (subxid <= xid)
        return;
    txn = ReorderBufferTXNByXid(rb, xid, true, &new_top, lsn, true);
    subtxn = ReorderBufferTXNByXid(rb, subxid, true, &new_sub, lsn, false);

    if (!new_sub) {
        if (rbtxn_is_known_subxact(subtxn)) {
            /* already associated, nothing to do */
            return;
        } else {
            /*
             * We already saw this transaction, but initially added it to the list
             * of top-level txns.  Now that we know it's not top-level, remove
             * it from there.
             */
            dlist_delete(&subtxn->node);
        }
    }
    subtxn->txn_flags |= RBTXN_IS_SUBXACT;
    subtxn->toplevel_xid = xid;
    Assert(subtxn->nsubtxns == 0);
    /* set the reference to top-level transaction */
    subtxn->toptxn = txn;
    /* add to subtransaction list */
    dlist_push_tail(&txn->subtxns, &subtxn->node);
    txn->nsubtxns++;

    /* Possibly transfer the subtxn's snapshot to its top-level txn. */
    ReorderBufferTransferSnapToParent(txn, subtxn);

    /* Verify LSN-ordering invariant */
    AssertTXNLsnOrder(rb);
}

/*
 * ReorderBufferTransferSnapToParent
 *     Transfer base snapshot from subtxn to top-level txn, if needed
 *
 * This is done if the top-level txn doesn't have a base snapshot, or if the
 * subtxn's base snapshot has an earlier LSN than the top-level txn's base
 * snapshot's LSN.  This can happen if there are no changes in the toplevel
 * txn but there are some in the subtxn, or the first change in subtxn has
 * earlier LSN than first change in the top-level txn and we learned about
 * their kinship only now.
 *
 * The subtransaction's snapshot is cleared regardless of the transfer
 * happening, since it's not needed anymore in either case.
 *
 * We do this as soon as we become aware of their kinship, to avoid queueing
 * extra snapshots to txns known-as-subtxns -- only top-level txns will
 * receive further snapshots.
 */
static void ReorderBufferTransferSnapToParent(ReorderBufferTXN *txn, ReorderBufferTXN *subtxn)
{
    Assert(subtxn->toplevel_xid == txn->xid);

    if (subtxn->base_snapshot != NULL) {
        if (txn->base_snapshot == NULL || subtxn->base_snapshot_lsn < txn->base_snapshot_lsn) {
            /*
             * If the toplevel transaction already has a base snapshot but
             * it's newer than the subxact's, purge it.
             */
            if (txn->base_snapshot != NULL) {
                SnapBuildSnapDecRefcount(txn->base_snapshot);
                dlist_delete(&txn->base_snapshot_node);
            }

            /*
             * The snapshot is now the top transaction's; transfer it, and
             * adjust the list position of the top transaction in the list by
             * moving it to where the subtransaction is.
             */
            txn->base_snapshot = subtxn->base_snapshot;
            txn->base_snapshot_lsn = subtxn->base_snapshot_lsn;
            dlist_insert_before(&subtxn->base_snapshot_node, &txn->base_snapshot_node);

            /*
             * The subtransaction doesn't have a snapshot anymore (so it
             * mustn't be in the list.)
             */
            subtxn->base_snapshot = NULL;
            subtxn->base_snapshot_lsn = InvalidXLogRecPtr;
            dlist_delete(&subtxn->base_snapshot_node);
        } else {
            /* Base snap of toplevel is fine, so subxact's is not needed */
            SnapBuildSnapDecRefcount(subtxn->base_snapshot);
            dlist_delete(&subtxn->base_snapshot_node);
            subtxn->base_snapshot = NULL;
            subtxn->base_snapshot_lsn = InvalidXLogRecPtr;
        }
    }
}

/*
 * Associate a subtransaction with its toplevel transaction at commit
 * time. There may be no further changes added after this.
 */
void ReorderBufferCommitChild(ReorderBuffer *rb, TransactionId xid, TransactionId subxid, XLogRecPtr commit_lsn,
                              XLogRecPtr end_lsn)
{
    ReorderBufferTXN *subtxn = NULL;

    subtxn = ReorderBufferTXNByXid(rb, subxid, false, NULL, InvalidXLogRecPtr, false);
    /*
     * No need to do anything if that subtxn didn't contain any changes
     */
    if (subtxn == NULL)
        return;

    subtxn->final_lsn = commit_lsn;
    subtxn->end_lsn = end_lsn;
    /*
     * Assign this subxact as a child of the toplevel xact (no-op if already
     * done.)
     */
    ReorderBufferAssignChild(rb, xid, subxid, InvalidXLogRecPtr);
}

/*
 * Support for efficiently iterating over a transaction's and its
 * subtransactions' changes.
 *
 * We do by doing a k-way merge between transactions/subtransactions. For that
 * we model the current heads of the different transactions as a binary heap
 * so we easily know which (sub-)transaction has the change with the smallest
 * lsn next.
 *
 * We assume the changes in individual transactions are already sorted by LSN.
 *
 *
 * Binary heap comparison function.
 */
static int ReorderBufferIterCompare(Datum a, Datum b, void *arg)
{
    ReorderBufferIterTXNState *state = (ReorderBufferIterTXNState *)arg;
    XLogRecPtr pos_a = state->entries[DatumGetInt32(a)].lsn;
    XLogRecPtr pos_b = state->entries[DatumGetInt32(b)].lsn;
    if (XLByteLT(pos_a, pos_b))
        return 1;
    else if (XLByteEQ(pos_a, pos_b))
        return 0;
    return -1;
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a
 * transaction and all its subtransactions.
 */
static ReorderBufferIterTXNState *ReorderBufferIterTXNInit(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    Size nr_txns = 0;
    ReorderBufferIterTXNState *state = 0;
    dlist_iter cur_txn_i;
    Size off;

    /*
     * Calculate the size of our heap: one element for every transaction that
     * contains changes.  (Besides the transactions already in the reorder
     * buffer, we count the one we were directly passed.)
     */
    if (txn->nentries > 0)
        nr_txns++;

    dlist_foreach(cur_txn_i, &txn->subtxns)
    {
        ReorderBufferTXN *cur_txn = NULL;

        cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
        if (cur_txn->nentries > 0)
            nr_txns++;
    }

    /*
     * description: Consider adding fastpath for the rather common nr_txns=1 case, no
     * need to allocate/build a heap then.
     *
     * allocate iteration state
     */
    state = (ReorderBufferIterTXNState *)MemoryContextAllocZero(rb->context,
                                                                sizeof(ReorderBufferIterTXNState) +
                                                                    sizeof(ReorderBufferIterTXNEntry) * nr_txns);

    state->nr_txns = nr_txns;
    dlist_init(&state->old_change);

    for (off = 0; off < state->nr_txns; off++) {
        state->entries[off].fd = -1;
        state->entries[off].segno = 0;
    }

    /* allocate heap */
    state->heap = binaryheap_allocate(state->nr_txns, ReorderBufferIterCompare, state);

    /*
     * Now insert items into the binary heap, in an unordered fashion.  (We
     * will run a heap assembly step at the end; this is more efficient.)
     */
    off = 0;

    /* add toplevel transaction if it contains changes */
    if (txn->nentries > 0) {
        ReorderBufferChange *cur_change = NULL;

        if (rbtxn_is_serialized(txn)) {
            /* serialize remaining changes */
            ReorderBufferSerializeTXN(rb, txn);
            (void)ReorderBufferRestoreChanges(rb, txn, &state->entries[off].fd, &state->entries[off].segno);
        }

        cur_change = dlist_head_element(ReorderBufferChange, node, &txn->changes);

        state->entries[off].lsn = cur_change->lsn;
        state->entries[off].change = cur_change;
        state->entries[off].txn = txn;

        binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
    }

    /* add subtransactions if they contain changes */
    dlist_foreach(cur_txn_i, &txn->subtxns)
    {
        ReorderBufferTXN *cur_txn = NULL;

        cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);
        if (cur_txn->nentries > 0) {
            ReorderBufferChange *cur_change = NULL;

            if (rbtxn_is_serialized(cur_txn)) {
                /* serialize remaining changes */
                ReorderBufferSerializeTXN(rb, cur_txn);
                (void)ReorderBufferRestoreChanges(rb, cur_txn, &state->entries[off].fd, &state->entries[off].segno);
            }

            cur_change = dlist_head_element(ReorderBufferChange, node, &cur_txn->changes);

            state->entries[off].lsn = cur_change->lsn;
            state->entries[off].change = cur_change;
            state->entries[off].txn = cur_txn;

            binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
        }
    }

    /* assemble a valid binary heap */
    binaryheap_build(state->heap);

    return state;
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransactions.
 *
 * Returns NULL when no further changes exist.
 */
static ReorderBufferChange *ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state)
{
    ReorderBufferChange *change = NULL;
    ReorderBufferIterTXNEntry *entry = NULL;
    int32 off = 0;

    /* nothing there anymore */
    if (state->heap->bh_size == 0)
        return NULL;

    off = DatumGetInt32(binaryheap_first(state->heap));
    entry = &state->entries[off];

    /* free memory we might have "leaked" in the previous *Next call */
    if (!dlist_is_empty(&state->old_change)) {
        change = dlist_container(ReorderBufferChange, node, dlist_pop_head_node(&state->old_change));
        ReorderBufferReturnChange(rb, change);
        Assert(dlist_is_empty(&state->old_change));
    }

    change = entry->change;

    /*
     * update heap with information about which transaction has the next
     * relevant change in LSN order
     *
     * there are in-memory changes
     */
    if (dlist_has_next(&entry->txn->changes, &entry->change->node)) {
        dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
        ReorderBufferChange *next_change = dlist_container(ReorderBufferChange, node, next);

        /* txn stays the same */
        state->entries[off].lsn = next_change->lsn;
        state->entries[off].change = next_change;

        binaryheap_replace_first(state->heap, Int32GetDatum(off));
        return change;
    }

    /* try to load changes from disk */
    if (entry->txn->nentries != entry->txn->nentries_mem) {
        /*
         * Ugly: restoring changes will reuse *Change records, thus delete the
         * current one from the per-tx list and only free in the next call.
         */
        dlist_delete(&change->node);
        dlist_push_tail(&state->old_change, &change->node);

        if (ReorderBufferRestoreChanges(rb, entry->txn, &entry->fd, &state->entries[off].segno)) {
            /* successfully restored changes from disk */
            ReorderBufferChange *next_change = dlist_head_element(ReorderBufferChange, node, &entry->txn->changes);

            ereport(DEBUG2, (errmodule(MOD_LOGICAL_DECODE), errmsg("restored %u/%u changes from disk",
                (uint32)entry->txn->nentries_mem, (uint32)entry->txn->nentries)));

            Assert(entry->txn->nentries_mem);
            /* txn stays the same */
            state->entries[off].lsn = next_change->lsn;
            state->entries[off].change = next_change;
            binaryheap_replace_first(state->heap, Int32GetDatum(off));

            return change;
        }
    }

    /* ok, no changes there anymore, remove */
    (void)binaryheap_remove_first(state->heap);

    return change;
}

/*
 * Deallocate the iterator
 */
static void ReorderBufferIterTXNFinish(ReorderBuffer *rb, ReorderBufferIterTXNState *state)
{
    Size off;

    for (off = 0; off < state->nr_txns; off++) {
        if (state->entries[off].fd != -1) {
            (void)CloseTransientFile(state->entries[off].fd);
        }
    }

    /* free memory we might have "leaked" in the last *Next call */
    if (!dlist_is_empty(&state->old_change)) {
        ReorderBufferChange *change = NULL;

        change = dlist_container(ReorderBufferChange, node, dlist_pop_head_node(&state->old_change));
        ReorderBufferReturnChange(rb, change);
        Assert(dlist_is_empty(&state->old_change));
    }

    binaryheap_free(state->heap);
    pfree(state);
    state = NULL;
}

/*
 * Cleanup the contents of a transaction, usually after the transaction
 * committed or aborted.
 */
static void ReorderBufferCleanupTXNInner(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr lsn, int nestlevel)
{
    bool found = false;
    dlist_mutable_iter iter;
    Size mem_freed = 0;

    if (nestlevel == 0) {
        /* cleanup subtransactions & their changes */
        dlist_foreach_modify(iter, &txn->subtxns)
        {
            ReorderBufferTXN *subtxn = NULL;

            subtxn = dlist_container(ReorderBufferTXN, node, iter.cur);

            /*
             * Subtransactions are always associated to the toplevel TXN, even if
             * they originally were happening inside another subtxn, so we won't
             * ever recurse more than one level deep here.
             */
            Assert(rbtxn_is_known_subxact(subtxn));
            Assert(subtxn->nsubtxns == 0);

            ReorderBufferCleanupTXNInner(rb, subtxn, lsn, nestlevel + 1);
        }
    }

    /* cleanup changes in the toplevel txn */
    dlist_foreach_modify(iter, &txn->changes)
    {
        ReorderBufferChange *change = NULL;

        change = dlist_container(ReorderBufferChange, node, iter.cur);

        /*
         * Instead of updating the memory counter for individual changes,
         * we sum up the size of memory to free so we can update the memory
         * counter all together below. This saves costs of maintaining
         * the max-heap.
         */
        mem_freed += ReorderBufferChangeSize(change);

        ReorderBufferReturnChange(rb, change, false);
    }

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, mem_freed);

    /*
     * Cleanup the tuplecids we stored for decoding catalog snapshot
     * access. They are always stored in the toplevel transaction.
     */
    dlist_foreach_modify(iter, &txn->tuplecids)
    {
        ReorderBufferChange *change = NULL;

        change = dlist_container(ReorderBufferChange, node, iter.cur);
        Assert(change->action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
        ReorderBufferReturnChange(rb, change);
    }
    /*
     * Cleanup the base snapshot, if set.
     */
    if (txn->base_snapshot != NULL) {
        SnapBuildSnapDecRefcount(txn->base_snapshot);
        dlist_delete(&txn->base_snapshot_node);
    }

    /*
     * Cleanup the snapshot for the last streamed run.
     */
    if (txn->snapshot_now != NULL) {
        Assert(rbtxn_is_streamed(txn));
        ReorderBufferFreeSnap(rb, txn->snapshot_now);
    }

    /*
     * Remove TXN from its containing list.
     *
     * Note: if txn is known as subxact, we are deleting the TXN from its
     * parent's list of known subxacts; this leaves the parent's nsubxacts
     * count too high, but we don't care.  Otherwise, we are deleting the TXN
     * from the LSN-ordered list of toplevel TXNs.
     */
    dlist_delete(&txn->node);

    /* now remove reference from buffer */
    (void)hash_search(rb->by_txn, (void *)&txn->xid, HASH_REMOVE, &found);
    Assert(found);

    /* remove entries spilled to disk */
    if (rbtxn_is_serialized(txn))
        ReorderBufferRestoreCleanup(rb, txn, lsn);

    /* deallocate */
    ReorderBufferReturnTXN(rb, txn);
}

void ReorderBufferCleanupTXN(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr lsn = InvalidXLogRecPtr)
{
    ReorderBufferCleanupTXNInner(rb, txn, lsn, 0);
}

/*
 * Build a hash with a (relfilenode, ctid) -> (cmin, cmax) mapping for use by
 * HeapTupleSatisfiesHistoricMVCC.
 */
static void ReorderBufferBuildTupleCidHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    dlist_iter iter;
    HASHCTL hash_ctl;
    int rc = 0;

    if (!rbtxn_has_catalog_changes(txn) || dlist_is_empty(&txn->tuplecids))
        return;

    if (NULL == txn->tuplecid_hash) {
        /*
         * we don't release 'txn->tuplecid_hash' after each streamed data
         * for user may generate large amount of DDL (like partition op) in one txn
         * which is O(n^2) 
         */
        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "", "");

        hash_ctl.keysize = sizeof(ReorderBufferTupleCidKey);
        hash_ctl.entrysize = sizeof(ReorderBufferTupleCidEnt);
        hash_ctl.hash = ReorderBufferTupleCidKeyHash;
        hash_ctl.match = ReorderBufferTupleCidKeyMatch;
        hash_ctl.hcxt = rb->context;
        /*
         * create the hash with the exact number of to-be-stored tuplecids from
         * the start
         */
        txn->tuplecid_hash = hash_create("ReorderBufferTupleCid", 2048, &hash_ctl,
                                         HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
    }

    dlist_foreach(iter, &txn->tuplecids)
    {
        ReorderBufferTupleCidKey key;
        ReorderBufferTupleCidEnt *ent = NULL;
        bool found = false;
        ReorderBufferChange *change = NULL;

        change = dlist_container(ReorderBufferChange, node, iter.cur);

        Assert(change->action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

        /* be careful about padding */
        rc = memset_s(&key, sizeof(ReorderBufferTupleCidKey), 0, sizeof(ReorderBufferTupleCidKey));
        securec_check(rc, "", "");

        key.relnode = change->data.tuplecid.node;

        ItemPointerCopy(&change->data.tuplecid.tid, &key.tid);

        ent = (ReorderBufferTupleCidEnt *)hash_search(txn->tuplecid_hash, (void *)&key,
                                                      HASHACTION(HASH_ENTER | HASH_FIND), &found);
        if (!found) {
            ent->cmin = change->data.tuplecid.cmin;
            ent->cmax = change->data.tuplecid.cmax;
            ent->combocid = change->data.tuplecid.combocid;
        } else {
            /*
             * if the tuple got valid in this transaction and now got deleted
             * we already have a valid cmin stored. The cmax will be
             * InvalidCommandId though.
             */
            ent->cmax = change->data.tuplecid.cmax;
        }
    }
}

/*
 * Copy a provided snapshot so we can modify it privately. This is needed so
 * that catalog modifying transactions can look into intermediate catalog
 * states.
 */
static Snapshot ReorderBufferCopySnap(ReorderBuffer *rb, Snapshot orig_snap, ReorderBufferTXN *txn, CommandId cid)
{
    Snapshot snap;
    dlist_iter iter;
    int i = 0;
    Size size;
    int rc = 0;
    size = sizeof(SnapshotData) + sizeof(TransactionId) * orig_snap->xcnt + sizeof(TransactionId) * (txn->nsubtxns + 1);

    snap = (SnapshotData *)MemoryContextAllocZero(rb->context, size);
    rc = memcpy_s(snap, sizeof(SnapshotData), orig_snap, sizeof(SnapshotData));
    securec_check(rc, "", "");
    snap->copied = true;
    snap->active_count = 1;
    snap->regd_count = 0;
    snap->xip = (TransactionId *)(snap + 1);

    if (snap->xcnt) {
        rc = memcpy_s(snap->xip, sizeof(TransactionId) * snap->xcnt, orig_snap->xip,
                      sizeof(TransactionId) * snap->xcnt);
        securec_check(rc, "", "");
    }
    /*
     * snap->subxip contains all txids that belong to our transaction which we
     * need to check via cmin/cmax. Thats why we store the toplevel
     * transaction in there as well.
     */
    snap->subxip = snap->xip + snap->xcnt;
    snap->subxip[i++] = txn->xid;

    /*
     * nsubxcnt isn't decreased when subtransactions abort, so count
     * manually. Since it's an upper boundary it is safe to use it for the
     * allocation above.
     */
    snap->subxcnt = 1;

    dlist_foreach(iter, &txn->subtxns)
    {
        ReorderBufferTXN *sub_txn = NULL;

        sub_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
        snap->subxip[i++] = sub_txn->xid;
        snap->subxcnt++;
    }

    /* sort so we can bsearch() later */
    qsort(snap->subxip, snap->subxcnt, sizeof(TransactionId), xidComparator);

    /* store the specified current CommandId */
    snap->curcid = cid;

    return snap;
}

/*
 * Free a previously ReorderBufferCopySnap'ed snapshot
 */
static void ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap)
{
    if (snap->copied) {
        if (GTM_LITE_MODE && (snap->prepared_array != NULL))
            pfree_ext(snap->prepared_array);

        pfree(snap);
        snap = NULL;
    } else
        SnapBuildSnapDecRefcount(snap);
}

/*
 * Helper function for ReorderBufferProcessTXN for applying the DDL message.
 */
static inline void
ReorderBufferApplyDDLMessage(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                ReorderBufferChange *change)
{
    if (change->data.ddl.cmdtype != DCT_NewPub || t_thrd.publication_cxt.isNewPubNeeded) {
        rb->ddl(rb, txn, change->lsn,
            change->data.ddl.prefix,
            change->data.ddl.relid,
            change->data.ddl.cmdtype,
            change->data.ddl.message_size,
            change->data.ddl.message);
    }
}

/*
 * Set xid to detect concurrent aborts.
 *
 * While streaming an in-progress transaction or decoding a prepared
 * transaction there is a possibility that the (sub)transaction might get
 * aborted concurrently.  In such case if the (sub)transaction has catalog
 * update then we might decode the tuple using wrong catalog version.  For
 * example, suppose there is one catalog tuple with (xmin: 500, xmax: 0).  Now,
 * the transaction 501 updates the catalog tuple and after that we will have
 * two tuples (xmin: 500, xmax: 501) and (xmin: 501, xmax: 0).  Now, if 501 is
 * aborted and some other transaction say 502 updates the same catalog tuple
 * then the first tuple will be changed to (xmin: 500, xmax: 502).  So, the
 * problem is that when we try to decode the tuple inserted/updated in 501
 * after the catalog update, we will see the catalog tuple with (xmin: 500,
 * xmax: 502) as visible because it will consider that the tuple is deleted by
 * xid 502 which is not visible to our snapshot.  And when we will try to
 * decode with that catalog tuple, it can lead to a wrong result or a crash.
 * So, it is necessary to detect concurrent aborts to allow streaming of
 * in-progress transactions or decoding of prepared  transactions.
 *
 * For detecting the concurrent abort we set CheckXidAlive to the current
 * (sub)transaction's xid for which this change belongs to.  And, during
 * catalog scan we can check the status of the xid and if it is aborted we will
 * report a specific error so that we can stop streaming current transaction
 * and discard the already streamed changes on such an error.  We might have
 * already streamed some of the changes for the aborted (sub)transaction, but
 * that is fine because when we decode the abort we will stream abort message
 * to truncate the changes in the subscriber. Similarly, for prepared
 * transactions, we stop decoding if concurrent abort is detected and then
 * rollback the changes when rollback prepared is encountered. See
 * DecodePrepare.
 */
static inline void SetupCheckXidLive(TransactionId xid)
{
    /*
     * If the input transaction id is already set as a CheckXidAlive then
     * nothing to do.
     */
    if (TransactionIdEquals(u_sess->utils_cxt.CheckXidAlive, xid))
        return;

    /*
     * setup CheckXidAlive if it's not committed yet.  We don't check if the
     * xid is aborted.  That will happen during catalog access.
     */
    if (!TransactionIdDidCommit(xid))
        u_sess->utils_cxt.CheckXidAlive = xid;
    else
        u_sess->utils_cxt.CheckXidAlive = InvalidTransactionId;
}

/*
 * Function to store the command id and snapshot at the end of the current
 * stream so that we can reuse the same while sending the next stream.
 */
static inline void ReorderBufferSaveTXNSnapshot(
    ReorderBuffer *rb, ReorderBufferTXN *txn, Snapshot snapshot_now, CommandId command_id)
{
    txn->command_id = command_id;

    /* Avoid copying if it's already copied. */
    if (snapshot_now->copied)
        txn->snapshot_now = snapshot_now;
    else
        txn->snapshot_now = ReorderBufferCopySnap(rb, snapshot_now, txn, command_id);
}

/*
 * Mark the given transaction as streamed if it's a top-level transaction
 * or has changes.
 */
static void ReorderBufferMaybeMarkTXNStreamed(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    /*
     * The top-level transaction, is marked as streamed always, even if it
     * does not contain any changes (that is, when all the changes are in
     * subtransactions).
     *
     * For subtransactions, we only mark them as streamed when there are
     * changes in them.
     *
     * We do it this way because of aborts - we don't want to send aborts for
     * XIDs the downstream is not aware of. And of course, it always knows
     * about the top-level xact (we send the XID in all messages), but we
     * never stream XIDs of empty subxacts.
     */
    if (rbtxn_is_toptxn(txn) || (txn->nentries_mem != 0))
        txn->txn_flags |= RBTXN_IS_STREAMED;
}

/*
 * Discard changes from a transaction (and subtransactions), either after
 * streaming, decoding them at PREPARE, or detecting the transaction abort.
 * Keep the remaining info - transactions, tuplecids, invalidations and
 * snapshots.
 *
 * We additionally remove tuplecids after decoding the transaction at prepare
 * time as we only need to perform invalidation at rollback or commit prepared.
 *
 * 'txn_prepared' indicates that we have decoded the transaction at prepare
 * time.
 */
static void ReorderBufferTruncateTXN(
    ReorderBuffer *rb, ReorderBufferTXN *txn, bool txn_prepared)
{
    dlist_mutable_iter iter;
    Size mem_freed = 0;

    /* cleanup subtransactions & their changes */
    dlist_foreach_modify(iter, &txn->subtxns)
    {
        ReorderBufferTXN *subtxn;

        subtxn = dlist_container(ReorderBufferTXN, node, iter.cur);

        /*
         * Subtransactions are always associated to the toplevel TXN, even if
         * they originally were happening inside another subtxn, so we won't
         * ever recurse more than one level deep here.
         */
        Assert(rbtxn_is_known_subxact(subtxn));
        Assert(subtxn->nsubtxns == 0);

        ReorderBufferMaybeMarkTXNStreamed(rb, subtxn);
        ReorderBufferTruncateTXN(rb, subtxn, txn_prepared);
    }

    /* cleanup changes in the txn */
    dlist_foreach_modify(iter, &txn->changes)
    {
        ReorderBufferChange *change;

        change = dlist_container(ReorderBufferChange, node, iter.cur);

        /* Check we're not mixing changes from different transactions. */
        Assert(change->txn == txn);

        /* remove the change from it's containing list */
        dlist_delete(&change->node);

        /*
         * Instead of updating the memory counter for individual changes,
         * we sum up the size of memory to free so we can update the memory
         * counter all together below. This saves costs of maintaining
         * the max-heap.
         */
        mem_freed += ReorderBufferChangeSize(change);

        ReorderBufferReturnChange(rb, change, false);
    }

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, mem_freed);

    /*
     * If this is a prepared txn, cleanup the tuplecids we stored for
     * decoding catalog snapshot access. They are always stored in the
     * toplevel transaction.
     */
    dlist_foreach_modify(iter, &txn->tuplecids)
    {
        ReorderBufferChange *change;

        change = dlist_container(ReorderBufferChange, node, iter.cur);

        /* Check we're not mixing changes from different transactions. */
        Assert(change->txn == txn);
        Assert(change->action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

        /* Remove the change from its containing list. */
        dlist_delete(&change->node);

        ReorderBufferReturnChange(rb, change, true);
    }

    /* If this txn is serialized then clean the disk space. */
    if (rbtxn_is_serialized(txn)) {
        ReorderBufferRestoreCleanup(rb, txn, txn->final_lsn);
        txn->txn_flags &= ~RBTXN_IS_SERIALIZED;

        /*
         * We set this flag to indicate if the transaction is ever serialized.
         * We need this to accurately update the stats as otherwise the same
         * transaction can be counted as serialized multiple times.
         */
        txn->txn_flags |= RBTXN_IS_SERIALIZED_CLEAR;
    }

    /* also reset the number of entries in the transaction */
    txn->nentries_mem = 0;
    txn->nentries = 0;
}

/*
 * Check the transaction status by CLOG lookup and discard all changes if
 * the transaction is aborted. The transaction status is cached in
 * txn->txn_flags so we can skip future changes and avoid CLOG lookups on the
 * next call.
 *
 * Return true if the transaction is aborted, otherwise return false.
 */
static bool ReorderBufferCheckAndTruncateAbortedTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    /*
     * Quick return if the transaction status is already known.
     */
    if (rbtxn_is_committed(txn))
        return false;
    if (rbtxn_is_aborted(txn)) {
        /* Already-aborted transactions should not have any changes */
        Assert(txn->size == 0);

        return true;
    }

    /* Otherwise, check the transaction status using CLOG lookup */

    if (TransactionIdIsInProgress(txn->xid))
        return false;

    if (TransactionIdDidCommit(txn->xid)) {
        /*
         * Remember the transaction is committed so that we can skip CLOG
         * check next time, avoiding the pressure on CLOG lookup.
         */
        Assert(!rbtxn_is_aborted(txn));
        txn->txn_flags |= RBTXN_IS_COMMITTED;
        return false;
    }

    /*
     * The transaction aborted. We discard both the changes collected so far
     * and the toast reconstruction data. The full cleanup will happen as part
     * of decoding ABORT record of this transaction.
     */
    ReorderBufferTruncateTXN(rb, txn, false);
    ReorderBufferToastReset(rb, txn);

    /* All changes should be discarded */
    Assert(txn->size == 0);

    /*
     * Mark the transaction as aborted so we can ignore future changes of this
     * transaction.
     */
    Assert(!rbtxn_is_committed(txn));
    txn->txn_flags |= RBTXN_IS_ABORTED;

    return true;
}

/*
 * Helper function for ReorderBufferProcessTXN to handle the concurrent
 * abort of the streaming transaction.  This resets the TXN such that it
 * can be used to stream the remaining data of transaction being processed.
 * This can happen when the subtransaction is aborted and we still want to
 * continue processing the main or other subtransactions data.
 */
static void ReorderBufferResetTXN(
    ReorderBuffer *rb, ReorderBufferTXN *txn, Snapshot snapshot_now,
    CommandId command_id, XLogRecPtr last_lsn)
{
    /* Discard the changes that we just streamed */
    ReorderBufferTruncateTXN(rb, txn, false);

    /* Free all resources allocated for toast reconstruction */
    ReorderBufferToastReset(rb, txn);

    /*
     * For the streaming case, stop the stream and remember the command ID and
     * snapshot for the streaming run.
     */
    if (rbtxn_is_streamed(txn)) {
        rb->stream_stop(rb, txn, last_lsn);
        ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id);
    }

    /* All changes must be deallocated */
    Assert(txn->size == 0);
}

/*
 * Send data of a large transaction (and its subtransactions) to the
 * output plugin, but using the stream API.
 */
static void ReorderBufferStreamTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    Snapshot    snapshot_now;
    CommandId   command_id;
    Size        stream_bytes;
    bool        txn_is_streamed;

    /* We can never reach here for a subtransaction. */
    Assert(txn->toptxn == NULL);

    /*
     * We can't make any assumptions about base snapshot here, similar to what
     * ReorderBufferCommit() does. That relies on base_snapshot getting
     * transferred from subxact in ReorderBufferCommitChild(), but that was
     * not yet called as the transaction is in-progress.
     *
     * So just walk the subxacts and use the same logic here. But we only need
     * to do that once, when the transaction is streamed for the first time.
     * After that we need to reuse the snapshot from the previous run.
     *
     * Unlike DecodeCommit which adds xids of all the subtransactions in
     * snapshot's xip array via SnapBuildCommittedTxn, we can't do that here
     * but we do add them to subxip array instead via ReorderBufferCopySnap.
     * This allows the catalog changes made in subtransactions decoded till
     * now to be visible.
     */
    if (txn->snapshot_now == NULL) {
        dlist_iter  subxact_i;

        /* make sure this transaction is streamed for the first time */
        Assert(!rbtxn_is_streamed(txn));

        /* at the beginning we should have invalid command ID */
        Assert(txn->command_id == InvalidCommandId);

        dlist_foreach(subxact_i, &txn->subtxns) {
            ReorderBufferTXN *subtxn;
            subtxn = dlist_container(ReorderBufferTXN, node, subxact_i.cur);
            ReorderBufferTransferSnapToParent(txn, subtxn);
        }

        /*
         * If this transaction has no snapshot, it didn't make any changes to
         * the database till now, so there's nothing to decode.
         */
        if (txn->base_snapshot == NULL) {
            Assert(txn->ninvalidations == 0);
            return;
        }

        command_id = FirstCommandId;
        snapshot_now = ReorderBufferCopySnap(rb, txn->base_snapshot,
                                             txn, command_id);
    }
    else {
        /* the transaction must have been already streamed */
        Assert(rbtxn_is_streamed(txn));

        /*
         * Nah, we already have snapshot from the previous streaming run. We
         * assume new subxacts can't move the LSN backwards, and so can't beat
         * the LSN condition in the previous branch (so no need to walk
         * through subxacts again). In fact, we must not do that as we may be
         * using snapshot half-way through the subxact.
         */
        command_id = txn->command_id;

        /*
         * We can't use txn->snapshot_now directly because after the last
         * streaming run, we might have got some new sub-transactions. So we
         * need to add them to the snapshot.
         */
        snapshot_now = ReorderBufferCopySnap(rb, txn->snapshot_now,
                                             txn, command_id);

        /* Free the previously copied snapshot. */
        Assert(txn->snapshot_now->copied);
        ReorderBufferFreeSnap(rb, txn->snapshot_now);
        txn->snapshot_now = NULL;
    }

    /*
     * Remember this information to be used later to update stats. We can't
     * update the stats here as an error while processing the changes would
     * lead to the accumulation of stats even though we haven't streamed all
     * the changes.
     */
    txn_is_streamed = rbtxn_is_streamed(txn);
    stream_bytes = txn->total_size;

    /* Process and send the changes to output plugin. */
    ReorderBufferProcessTXN(rb, txn, InvalidXLogRecPtr, snapshot_now,
                            command_id, true);

    rb->streamCount += 1;
    rb->streamBytes += stream_bytes;

    /* Don't consider already streamed transaction. */
    rb->streamTxns += (txn_is_streamed) ? 0 : 1;

    Assert(dlist_is_empty(&txn->changes));
    Assert(txn->nentries == 0);
    Assert(txn->nentries_mem == 0);
}

/*
 * If the transaction was (partially) streamed, we need to prepare or commit
 * it in a 'streamed' way.  That is, we first stream the remaining part of the
 * transaction, and then invoke stream_prepare or stream_commit message as per
 * the case.
 */
static void ReorderBufferStreamCommit(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    /* we should only call this for previously streamed transactions */
    Assert(rbtxn_is_streamed(txn));

    ReorderBufferStreamTXN(rb, txn);

    rb->stream_commit(rb, txn, txn->final_lsn);
    ReorderBufferCleanupTXN(rb, txn);
}


/*
 * Perform the replay of a transaction and its non-aborted subtransactions.
 *
 * Subtransactions previously have to be processed by
 * ReorderBufferCommitChild(), even if previously assigned to the toplevel
 * transaction with ReorderBufferAssignChild.
 *
 * We currently can only decode a transaction's contents when its commit
 * record is read because that's the only place where we know about cache
 * invalidations. Thus, once a toplevel commit is read, we iterate over the top
 * and subtransactions (using a k-way merge) and replay the changes in lsn
 * order.
 */
static void ReorderBufferReplay(ReorderBufferTXN *txn,
    ReorderBuffer *rb, TransactionId xid, XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
    RepOriginId origin_id, XLogRecPtr origin_lsn, CommitSeqNo csn, TimestampTz commit_time)
{
    Snapshot    snapshot_now;
    CommandId   command_id = FirstCommandId;

    txn->final_lsn = commit_lsn;
    txn->end_lsn = end_lsn;
    txn->origin_id = origin_id;
    txn->origin_lsn = origin_lsn;
    txn->csn = csn;
    txn->commit_time = commit_time;

    /*
     * If the transaction was (partially) streamed, we need to commit it in a
     * 'streamed' way. That is, we first stream the remaining part of the
     * transaction, and then invoke stream_commit message.
     *
     * Called after everything (origin ID, LSN, ...) is stored in the
     * transaction to avoid passing that information directly.
     */
    if (rbtxn_is_streamed(txn)) {
        ReorderBufferStreamCommit(rb, txn);
        return;
    }

    /*
     * If this transaction has no snapshot, it didn't make any changes to the
     * database, so there's nothing to decode.  Note that
     * ReorderBufferCommitChild will have transferred any snapshots from
     * subtransactions if there were any.
     */
    if (txn->base_snapshot == NULL) {
        Assert(txn->ninvalidations == 0);
        ReorderBufferCleanupTXN(rb, txn);
        return;
    }

    snapshot_now = txn->base_snapshot;
    ReorderBufferProcessTXN(rb, txn, commit_lsn, snapshot_now, command_id, false);
}

void ReorderBufferCommit(ReorderBuffer *rb, TransactionId xid, int nsubxacts, TransactionId *sub_xids,
    XLogRecPtr commit_lsn, XLogRecPtr end_lsn, RepOriginId origin_id, XLogRecPtr origin_lsn, CommitSeqNo csn,
    TimestampTz commit_time)
{
    ReorderBufferTXN *txn = ReorderBufferTXNByXid(
        rb, xid, false, NULL, InvalidXLogRecPtr, false);

    if (txn) {
        txn->nr_subtxn_commit = nsubxacts;
        txn->subtxn_commit = sub_xids;
        ReorderBufferReplay(txn, rb, xid,
            commit_lsn, end_lsn, origin_id, origin_lsn, csn, commit_time);
    }
}

/*
 * Perform the replay of a transaction and its non-aborted subtransactions.
 *
 * Subtransactions previously have to be processed by
 * ReorderBufferCommitChild(), even if previously assigned to the toplevel
 * transaction with ReorderBufferAssignChild.
 *
 * We currently can only decode a transaction's contents when its commit
 * record is read because that's the only place where we know about cache
 * invalidations. Thus, once a toplevel commit is read, we iterate over the top
 * and subtransactions (using a k-way merge) and replay the changes in lsn
 * order.
 */
static void ReorderBufferProcessTXN(
    ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr commit_lsn, 
    volatile Snapshot snapshot_now, volatile CommandId command_id, bool streaming)
{
    MemoryContext ccxt = CurrentMemoryContext;
    ReorderBufferIterTXNState *volatile iterstate = NULL;
    ReorderBufferChange *change = NULL;
    volatile XLogRecPtr prev_lsn = InvalidXLogRecPtr;
    volatile bool stream_started = false;
    ReorderBufferTXN *volatile curtxn = NULL;
    /*
     *  Fix and improve cache invalidation logic for logical decoding.
     *
     *  There are basically three situations in which logical decoding needs
     *  to perform cache invalidation. During/After replaying a transaction
     *  with catalog changes, when skipping a uninteresting transaction that
     *  performed catalog changes and when erroring out while replaying a
     *  transaction. Unfortunately these three cases were all done slightly
     *  differently - partially because 8de3e410fa, which greatly simplifies
     *  matters, got committed in the midst of the development of logical
     *  decoding.
     *  
     *  The actually problematic case was when logical decoding skipped
     *  transaction commits (and thus processed invalidations). When used via
     *  the SQL interface cache invalidation could access the catalog - bad,
     *  because we didn't set up enough state to allow that correctly. It'd
     *  not be hard to setup sufficient state, but the simpler solution is to
     *  always perform cache invalidation outside a valid transaction.
     *  
     *  Also make the different cache invalidation cases look as similar as
     *  possible, to ease code review.
     */
    bool using_subtxn = IsTransactionOrTransactionBlock();

    if (commit_lsn == InvalidXLogRecPtr)
        txn->csn = InvalidCommitSeqNo;

    /* build data to be able to lookup the CommandIds of catalog tuples */
    ReorderBufferBuildTupleCidHash(rb, txn);

    /* setup the initial snapshot */
    SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash);

    PG_TRY();
    {
        /*
         * Decoding needs access to syscaches et al., which in turn use
         * heavyweight locks and such. Thus we need to have enough state around
         * to keep track of those. The easiest way is to simply use a
         * transaction internally. That also allows us to easily enforce that
         * nothing writes to the database by checking for xid assignments.
         *
         * When we're called via the SQL SRF there's already a transaction
         * started, so start an explicit subtransaction there.
         */
        if (using_subtxn) {
            BeginInternalSubTransaction(streaming ? "stream" : "replay");
        } else {
            StartTransactionCommand();
        }

        if (!streaming)
            rb->begin(rb, txn);

        iterstate = ReorderBufferIterTXNInit(rb, txn);
        while ((change = ReorderBufferIterTXNNext(rb, iterstate))) {
            Relation relation = NULL;
            Oid reloid;
            Oid partitionReltoastrelid = InvalidOid;
            bool isSegment = false;
            Oid relrewrite = InvalidOid;

            CHECK_FOR_INTERRUPTS();
            ResetLogicalStreamingState();

            /*
             * We can't call start stream callback before processing first
             * change.
             */
            if (prev_lsn == InvalidXLogRecPtr) {
                if (streaming) {
                    txn->origin_id = change->origin_id;
                    rb->stream_start(rb, txn, change->lsn);
                    stream_started = true;
                }
            }

            /*
             * Enforce correct ordering of changes, merged from multiple
             * subtransactions. The changes may have the same LSN due to
             * MULTI_INSERT xlog records.
             */
            Assert(prev_lsn == InvalidXLogRecPtr || prev_lsn <= change->lsn);

            prev_lsn = change->lsn;

            /*
             * Set the current xid to detect concurrent aborts. This is
             * required for the cases when we decode the changes before the
             * COMMIT record is processed.
             */
            if (streaming) {
                curtxn = change->txn;
                SetupCheckXidLive(curtxn->xid);
            }

            switch (change->action) {
                case REORDER_BUFFER_CHANGE_INSERT:
                case REORDER_BUFFER_CHANGE_UPDATE:
                case REORDER_BUFFER_CHANGE_DELETE:
                    u_sess->utils_cxt.HistoricSnapshot->snapshotcsn = change->data.tp.snapshotcsn;
                    Assert(snapshot_now);

                    isSegment = IsSegmentFileNode(change->data.tp.relnode);
                    reloid = HeapGetRelid(change->data.tp.relnode.spcNode, change->data.tp.relnode.relNode,
                        partitionReltoastrelid, NULL, isSegment);
                    /*
                     * Catalog tuple without data, emitted while catalog was
                     * in the process of being rewritten.
                     */
                    if (reloid == InvalidOid && change->data.tp.newtuple == NULL && change->data.tp.oldtuple == NULL)
                        continue;
                    else if (reloid == InvalidOid) {
                        /*
                         * description:
                         * When we try to decode a table who is already dropped.
                         * Maybe we could not find it relnode.In this time, we will undecode this log.
                         * It will be solve when we use MVCC.
                         */
                        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
                            errmsg("could not lookup relation %s",
                                   relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
                        continue;
                    }

                    relation = RelationIdGetRelation(reloid);
                    if (relation == NULL) {
                        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
                            errmsg("could open relation descriptor %s",
                                   relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
                        continue;
                    }

                    /*
                     * Do not decode private tables, otherwise there will be security problems.
                     */
                    if (is_role_independent(FindRoleid(reloid))) {
                        RelationClose(relation);
                        continue;
                    }

                    if (CSTORE_NAMESPACE == get_rel_namespace(RelationGetRelid(relation))) {
                        RelationClose(relation);
                        continue;
                    }
                    
                    relrewrite = RelationGetRelrewriteOption(relation);
                    if (RelationIsLogicallyLogged(relation) &&
                        (!OidIsValid(relrewrite) || rb->output_rewrites)) {
                        /*
                         * For now ignore sequence changes entirely. Most of
                         * the time they don't log changes using records we
                         * understand, so it doesn't make sense to handle the
                         * few cases we do.
                         */
                        if (RELKIND_IS_SEQUENCE(relation->rd_rel->relkind)) {
                        } else if (!IsToastRelation(relation)) { /* user-triggered change */
                            ReorderBufferToastReplace(rb, txn, relation, change, partitionReltoastrelid, false);
                            if (streaming)
                                rb->stream_change(rb, txn, relation, change);
                            else
                                rb->apply_change(rb, txn, relation, change);
                            /*
                             * Only clear reassembled toast chunks if we're
                             * sure they're not required anymore. The creator
                             * of the tuple tells us.
                             */
                            if (change->data.tp.clear_toast_afterwards)
                                ReorderBufferToastReset(rb, txn);
                        } else if (change->action == REORDER_BUFFER_CHANGE_INSERT) {
                            /* we're not interested in toast deletions
                             *
                             * Need to reassemble the full toasted Datum in
                             * memory, to ensure the chunks don't get reused
                             * till we're done remove it from the list of this
                             * transaction's changes. Otherwise it will get
                             * freed/reused while restoring spooled data from
                             * disk.
                             */
                            dlist_delete(&change->node);
                            ReorderBufferToastAppendChunk(rb, txn, relation, change, false);
                        }
                    }
                    RelationClose(relation);
                    break;
                case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
                    /* get rid of the old */
                    TeardownHistoricSnapshot(false);

                    if (snapshot_now->copied) {
                        ReorderBufferFreeSnap(rb, snapshot_now);
                        snapshot_now = NULL;
                        snapshot_now = ReorderBufferCopySnap(rb, change->data.snapshot, txn, command_id);
                    } else if (change->data.snapshot->copied) {
                        /*
                         * Restored from disk, need to be careful not to double
                         * free. We could introduce refcounting for that, but for
                         * now this seems infrequent enough not to care.
                         */
                        snapshot_now = ReorderBufferCopySnap(rb, change->data.snapshot, txn, command_id);
                    } else {
                        snapshot_now = change->data.snapshot;
                    }

                    /* and continue with the new one */
                    SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash);
                    break;

                case REORDER_BUFFER_CHANGE_INVALIDATION:
                    /* Execute the invalidation messages locally */
                    ReorderBufferExecuteInvalidations(
                        change->data.inval.ninvalidations,
                        change->data.inval.invalidations);
                    break;

                case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
                    Assert(change->data.command_id != InvalidCommandId);
                    if (command_id < change->data.command_id) {
                        command_id = change->data.command_id;

                        if (!snapshot_now->copied) {
                            /* we don't use the global one anymore */
                            snapshot_now = ReorderBufferCopySnap(rb, snapshot_now, txn, command_id);
                        }

                        snapshot_now->curcid = command_id;

                        TeardownHistoricSnapshot(false);
                        SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash);
                    }

                    break;

                case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
                    ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tuplecid value in changequeue")));
                    break;
                case REORDER_BUFFER_CHANGE_DDL:
                    ReorderBufferApplyDDLMessage(rb, txn, change);
                    break;
                case REORDER_BUFFER_CHANGE_TRUNCATE: {
                    int i;
                    int nrelids = change->data.truncate.nrelids;
                    int nrelations = 0;
                    Relation *relations;

                    relations = (Relation *)palloc0(nrelids * sizeof(Relation));
                    for (i = 0; i < nrelids; i++) {
                        Oid relid = change->data.truncate.relids[i];
                        Relation relation;

                        relation = RelationIdGetRelation(relid);
                        if (relation == NULL) {
                            elog(ERROR, "could not open relation with OID %u", relid);
                        }

                        if (!RelationIsLogicallyLogged(relation)) {
                            continue;
                        }

                        relations[nrelations++] = relation;
                    }

                    rb->apply_truncate(rb, txn, nrelations, relations, change);

                    for (i = 0; i < nrelations; i++)
                        RelationClose(relations[i]);

                } break;
                case REORDER_BUFFER_CHANGE_UINSERT:
                case REORDER_BUFFER_CHANGE_UDELETE:
                case REORDER_BUFFER_CHANGE_UUPDATE:
                    u_sess->utils_cxt.HistoricSnapshot->snapshotcsn = change->data.utp.snapshotcsn;
                    Assert(snapshot_now);

                    reloid = HeapGetRelid(change->data.utp.relnode.spcNode, change->data.utp.relnode.relNode,
                        partitionReltoastrelid, NULL, false);
                    /*
                     * Catalog tuple without data, emitted while catalog was
                     * in the process of being rewritten.
                     */
                    if (reloid == InvalidOid && change->data.utp.newtuple == NULL && change->data.utp.oldtuple == NULL)
                        continue;
                    else if (reloid == InvalidOid) {
                        /*
                         * description:
                         * When we try to decode a table who is already dropped.
                         * Maybe we could not find it relnode.In this time, we will undecode this log.
                         * It will be solve when we use MVCC.
                         */
                        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
                            errmsg("could not lookup relation %s",
                                   relpathperm(change->data.utp.relnode, MAIN_FORKNUM))));
                        continue;
                    }

                    relation = RelationIdGetRelation(reloid);
                    if (relation == NULL) {
                        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
                            errmsg("could open relation descriptor %s",
                                   relpathperm(change->data.utp.relnode, MAIN_FORKNUM))));
                        continue;
                    }

                    if (is_role_independent(FindRoleid(reloid))) {
                        RelationClose(relation);
                        continue;
                    }

                    if (CSTORE_NAMESPACE == get_rel_namespace(RelationGetRelid(relation))) {
                        RelationClose(relation);
                        continue;
                    }

                    if (RelationIsLogicallyLogged(relation)) {
                        /*
                         * For now ignore sequence changes entirely. Most of
                         * the time they don't log changes using records we
                         * understand, so it doesn't make sense to handle the
                         * few cases we do.
                         */
                        if (RELKIND_IS_SEQUENCE(relation->rd_rel->relkind)) {
                        } else if (!IsToastRelation(relation)) { /* user-triggered change */
                            ReorderBufferToastReplace(rb, txn, relation, change, partitionReltoastrelid, true);
                            if (streaming)
                                rb->stream_change(rb, txn, relation, change);
                            else
                                rb->apply_change(rb, txn, relation, change);
                            /*
                             * Only clear reassembled toast chunks if we're
                             * sure they're not required anymore. The creator
                             * of the tuple tells us.
                             */
                            if (change->data.utp.clear_toast_afterwards)
                                ReorderBufferToastReset(rb, txn);
                        } else if (change->action == REORDER_BUFFER_CHANGE_UINSERT) {
                            /* we're not interested in toast deletions
                             *
                             * Need to reassemble the full toasted Datum in
                             * memory, to ensure the chunks don't get reused
                             * till we're done remove it from the list of this
                             * transaction's changes. Otherwise it will get
                             * freed/reused while restoring spooled data from
                             * disk.
                             */
                            dlist_delete(&change->node);
                            ReorderBufferToastAppendChunk(rb, txn, relation, change, true);
                        }
                    }
                    RelationClose(relation);
                    break;
            }
        }
        ResetLogicalStreamingState();
        ReorderBufferIterTXNFinish(rb, iterstate);
        iterstate = NULL;

        /*
         * Update total transaction count and total bytes processed by the
         * transaction and its subtransactions. Ensure to not count the
         * streamed transaction multiple times.
         *
         * Note that the statistics computation has to be done after
         * ReorderBufferIterTXNFinish as it releases the serialized change
         * which we have already accounted in ReorderBufferIterTXNNext.
         */
        if (!rbtxn_is_streamed(txn))
            rb->totalTxns++;

        rb->totalBytes += txn->total_size;

        /*
         * Done with current changes, send the last message for this set of
         * changes depending upon streaming mode.
         */
        if (streaming) {
            if (stream_started) {
                rb->stream_stop(rb, txn, prev_lsn);
                stream_started = false;
            }
        }
        else {
            /* call commit callback */
            rb->commit(rb, txn, commit_lsn);
        }

        /* this is just a sanity check against bad output plugin behaviour */
        if (GetCurrentTransactionIdIfAny() != InvalidTransactionId)
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("output plugin used xid %lu", GetCurrentTransactionId())));

        if (streaming)
            ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id);
        else if (snapshot_now->copied)
            ReorderBufferFreeSnap(rb, snapshot_now);

        /* cleanup */
        TeardownHistoricSnapshot(false);

        ReorderBufferExecuteInvalidations(txn->ninvalidations, txn->invalidations);

        if (using_subtxn)
            RollbackAndReleaseCurrentSubTransaction();
        else
            AbortCurrentTransaction();

        /*
         * We are here due to one of the four reasons: 1. Decoding an
         * in-progress txn. 2. Decoding a prepared txn. 3. Decoding of a
         * prepared txn that was (partially) streamed. 4. Decoding a committed
         * txn.
         *
         * For 1, we allow truncation of txn data by removing the changes
         * already streamed but still keeping other things like invalidations,
         * snapshot, and tuplecids. For 2 and 3, we indicate
         * ReorderBufferTruncateTXN to do more elaborate truncation of txn
         * data as the entire transaction has been decoded except for commit.
         * For 4, as the entire txn has been decoded, we can fully clean up
         * the TXN reorder buffer.
         */
        if (streaming) {
            ReorderBufferMaybeMarkTXNStreamed(rb, txn);
            ReorderBufferTruncateTXN(rb, txn, false);
            /* Reset the CheckXidAlive */
            u_sess->utils_cxt.CheckXidAlive = InvalidTransactionId;
        } else {
            /* remove potential on-disk data, and deallocate */
            ReorderBufferCleanupTXN(rb, txn);
        }
    }
    PG_CATCH();
    {
        MemoryContext ecxt = MemoryContextSwitchTo(ccxt);
        ErrorData  *errdata = CopyErrorData();

        ResetLogicalStreamingState();
        /* description: Encapsulate cleanup from the PG_TRY and PG_CATCH blocks */
        if (iterstate != NULL)
            ReorderBufferIterTXNFinish(rb, iterstate);

        TeardownHistoricSnapshot(true);

        if (using_subtxn)
            RollbackAndReleaseCurrentSubTransaction();
        else
            AbortCurrentTransaction();

        /*
         * Invalidations in an aborted transactions aren't allowed to do
         * catalog access, so we don't need to still have the snapshot setup.
         */
        ReorderBufferExecuteInvalidations(txn->ninvalidations, txn->invalidations);

        /*
         * The error code ERRCODE_TRANSACTION_LOGICALDECODING_ROLLBACK indicates a concurrent
         * abort of the (sub)transaction we are streaming or preparing. We
         * need to do the cleanup and return gracefully on this error, see
         * SetupCheckXidLive.
         *
         * This error code can be thrown by one of the callbacks we call
         * during decoding so we need to ensure that we return gracefully only
         * when we are sending the data in streaming mode and the streaming is
         * not finished yet or when we are sending the data out on a PREPARE
         * during a two-phase commit.
         */
        if (errdata->sqlerrcode == ERRCODE_TRANSACTION_LOGICALDECODING_ROLLBACK && stream_started) {
            /* curtxn must be set for streaming or prepared transactions */
            Assert(curtxn);

            /* Cleanup the temporary error state. */
            FlushErrorState();
            FreeErrorData(errdata);
            errdata = NULL;

            /* Remember the transaction is aborted. */
            Assert(!rbtxn_is_committed(curtxn));
            curtxn->txn_flags |= RBTXN_IS_ABORTED;

            /* Mark the transaction is streamed if appropriate */
            if (stream_started)
                ReorderBufferMaybeMarkTXNStreamed(rb, txn);

            /* Reset the TXN so that it is allowed to stream remaining data. */
            ReorderBufferResetTXN(rb, txn, snapshot_now, command_id, prev_lsn);
        }
        else {
            ReorderBufferCleanupTXN(rb, txn);
            MemoryContextSwitchTo(ecxt);
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
}

/*
 * Abort a transaction that possibly has previous changes. Needs to be first
 * called for subtransactions and then for the toplevel xid.
 *
 * NB: Transactions handled here have to have actively aborted (i.e. have
 * produced an abort record). Implicitly aborted transactions are handled via
 * ReorderBufferAbortOld(); transactions we're just not interesteded in, but
 * which have committed are handled in ReorderBufferForget().
 *
 * This function purges this transaction and its contents from memory and
 * disk.
 */
void ReorderBufferAbort(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);
    /* unknown, nothing to remove */
    if (txn == NULL)
        return;

    /* For streamed transactions notify the remote node about the abort. */
    if (rbtxn_is_streamed(txn)) {
        rb->stream_abort(rb, txn, lsn);

        /*
         * We might have decoded changes for this transaction that could load
         * the cache as per the current transaction's view (consider DDL's
         * happened in this transaction). We don't want the decoding of future
         * transactions to use those cache entries so execute invalidations.
         */
        if (txn->ninvalidations > 0)
            ReorderBufferImmediateInvalidation(
                rb, txn->ninvalidations, txn->invalidations);
    }

    /* cosmetic... */
    txn->final_lsn = lsn;

    /* remove potential on-disk data, and deallocate */
    ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Abort all transactions that aren't actually running anymore because the
 * server restarted.
 *
 * NB: These really have to be transactions that have aborted due to a server
 * crash/immediate restart, as we don't deal with invalidations here.
 */
void ReorderBufferAbortOld(ReorderBuffer *rb, TransactionId oldestRunningXid, XLogRecPtr lsn)
{
    dlist_mutable_iter it;

    /*
     * Iterate through all (potential) toplevel TXNs and abort all that are
     * older than what possibly can be running. Once we've found the first
     * that is alive we stop, there might be some that acquired an xid earlier
     * but started writing later, but it's unlikely and they will cleaned up
     * in a later call to ReorderBufferAbortOld().
     */
    dlist_foreach_modify(it, &rb->toplevel_by_lsn)
    {
        ReorderBufferTXN *txn = NULL;

        txn = dlist_container(ReorderBufferTXN, node, it.cur);
        if (TransactionIdPrecedes(txn->xid, oldestRunningXid)) {
            ereport(DEBUG2, (errmodule(MOD_LOGICAL_DECODE), errmsg("aborting old transaction %lu", txn->xid)));

            /* Notify the remote node about the crash/immediate restart. */
            if (rbtxn_is_streamed(txn))
                rb->stream_abort(rb, txn, InvalidXLogRecPtr);

            /* remove potential on-disk data, and deallocate this tx */
            ReorderBufferCleanupTXN(rb, txn, lsn);
        } else
            return;
    }
}

/*
 * Forget the contents of a transaction if we aren't interested in it's
 * contents. Needs to be first called for subtransactions and then for the
 * toplevel xid.
 *
 * This is significantly different to ReorderBufferAbort() because
 * transactions that have committed need to be treated differenly from aborted
 * ones since they may have modified the catalog.
 *
 * Note that this is only allowed to be called in the moment a transaction
 * commit has just been read, not earlier; otherwise later records referring
 * to this xid might re-create the transaction incompletely.
 */
void ReorderBufferForget(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);
    /* unknown, nothing to forget */
    if (txn == NULL)
        return;

    /* this transaction mustn't be streamed */
    Assert(!rbtxn_is_streamed(txn));

    /* 
     * Defense coding: Although streamed transaction should never be forgotten,
     * we abort here in release build to avoid resource leaks.
     */
    if (unlikely(rbtxn_is_streamed(txn))) {
        ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("unexpectedly forgetting a streamed transaction " XID_FMT ", sending stream_abort",
            txn->xid)));
        rb->stream_abort(rb, txn, lsn);
    }

    /* cosmetic... */
    txn->final_lsn = lsn;

    /*
     * Proccess cache invalidation messages if there are any. Even if we're
     * not interested in the transaction's contents, it could have manipulated
     * the catalog and we need to update the caches according to that.
     */
    if (txn->base_snapshot != NULL && txn->ninvalidations > 0) {
        /* setup snapshot to perform the invalidations in */
        ReorderBufferImmediateInvalidation(rb, txn->ninvalidations, txn->invalidations);
    } else
        Assert(txn->ninvalidations == 0);

    /* remove potential on-disk data, and deallocate */
    ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Execute invalidations happening outside the context of a decoded
 * transaction. That currently happens either for xid-less commits
 * (cf. RecordTransactionCommit()) or for invalidations in uninteresting
 * transactions (via ReorderBufferForget()).
 */
void ReorderBufferImmediateInvalidation(ReorderBuffer *rb, uint32 ninvalidations,
                                   SharedInvalidationMessage *invalidations)
{
    bool use_subtxn = IsTransactionOrTransactionBlock();
    uint32 i;

    if (use_subtxn)
        BeginInternalSubTransaction("replay");

    /*
     * Force invalidations to happen outside of a valid transaction - that way
     * entries will just be marked as invalid without accessing the catalog.
     * That's advantageous because we don't need to setup the full state
     * necessary for catalog access.
     */
    for (i = 0; i < ninvalidations; i++)
        LocalExecuteThreadAndSessionInvalidationMessage(&invalidations[i]);

    if (use_subtxn)
        RollbackAndReleaseCurrentSubTransaction();
}

/*
 * Tell reorderbuffer about an xid seen in the WAL stream. Has to be called at
 * least once for every xid in XLogRecord->xl_xid (other places in records
 * may, but do not have to be passed through here).
 *
 * Reorderbuffer keeps some datastructures about transactions in LSN order,
 * for efficiency. To do that it has to know about when transactions are seen
 * first in the WAL. As many types of records are not actually interesting for
 * logical decoding, they do not necessarily pass though here.
 */
void ReorderBufferProcessXid(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
    /* many records won't have an xid assigned, centralize check here */
    if (xid != InvalidTransactionId) {
        (void)ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);
    }
}

/*
 * Add a new snapshot to this transaction that may only used after lsn 'lsn'
 * because the previous snapshot doesn't describe the catalog correctly for
 * following rows.
 */
void ReorderBufferAddSnapshot(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn, Snapshot snap)
{
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);

    change->data.snapshot = snap;
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT;

    ReorderBufferQueueChange(ctx, xid, lsn, change);
}

/*
 * Set up the transaction's base snapshot.
 *
 * If we know that xid is a subtransaction, set the base snapshot on the
 * top-level transaction instead.
 */
void ReorderBufferSetBaseSnapshot(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Snapshot snap)
{
    ReorderBufferTXN *txn = NULL;
    bool is_new = false;
    AssertArg(snap != NULL);

    /*
     * Fetch the transaction to operate on.  If we know it's a subtransaction,
     * operate on its top-level transaction instead.
     */
    txn = ReorderBufferTXNByXid(rb, xid, true, &is_new, lsn, true);
    if (txn != NULL && rbtxn_is_known_subxact(txn))
        txn = ReorderBufferTXNByXid(rb, txn->toplevel_xid, false, NULL, InvalidXLogRecPtr, false);
    if (txn == NULL)
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not found txn which xid = %lu", xid)));
    Assert(txn->base_snapshot == NULL);

    txn->base_snapshot = snap;
    txn->base_snapshot_lsn = lsn;
    dlist_push_tail(&rb->txns_by_base_snapshot_lsn, &txn->base_snapshot_node);

    AssertTXNLsnOrder(rb);
}

/*
 * Access the catalog with this CommandId at this point in the changestream.
 *
 * May only be called for command ids > 1
 */
void ReorderBufferAddNewCommandId(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn, CommandId cid)
{
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);

    change->data.command_id = cid;
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;

    ReorderBufferQueueChange(ctx, xid, lsn, change);
}

/*
 * Add new (relfilenode, tid) -> (cmin, cmax) mappings.
 */
void ReorderBufferAddNewTupleCids(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, const RelFileNode &node,
                                  const ItemPointerData &tid, CommandId cmin, CommandId cmax, CommandId combocid)
{
    ReorderBufferChange *change = ReorderBufferGetChange(rb);
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

    change->data.tuplecid.node = node;
    change->data.tuplecid.tid = tid;
    change->data.tuplecid.cmin = cmin;
    change->data.tuplecid.cmax = cmax;
    change->data.tuplecid.combocid = combocid;
    change->lsn = lsn;
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID;

    change->txn = txn;
    dlist_push_tail(&txn->tuplecids, &change->node);
    txn->ntuplecids++;
}

/*
 * Setup the invalidation of the toplevel transaction.
 *
 * This needs to be called for each XLOG_XACT_INVALIDATIONS message and
 * accumulates all the invalidation messages in the toplevel transaction.
 * This is required because in some cases where we skip processing the
 * transaction (see ReorderBufferForget), we need to execute all the
 * invalidations together.
 */
void ReorderBufferAddInvalidations(LogicalDecodingContext *ctx, TransactionId xid, XLogRecPtr lsn, Size nmsgs,
                                   SharedInvalidationMessage *msgs)
{
    ReorderBufferTXN *txn = NULL;
    ReorderBuffer *rb = ctx->reorder;
    MemoryContext oldcontext;
    ReorderBufferChange *change;

    txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);
    oldcontext = MemoryContextSwitchTo(rb->context);

    /*
     * Collect all the invalidations under the top transaction so that we can
     * execute them all together.  See comment atop this function
     */
    txn = rbtxn_get_toptxn(txn);

    Assert(nmsgs > 0);

    /* Accumulate invalidations. */
    if (txn->ninvalidations == 0) {
        txn->ninvalidations = nmsgs;
        txn->invalidations = (SharedInvalidationMessage *)
            palloc(sizeof(SharedInvalidationMessage) * nmsgs);
        memcpy(txn->invalidations, msgs,
               sizeof(SharedInvalidationMessage) * nmsgs);
    } else {
        txn->invalidations = (SharedInvalidationMessage *)
            repalloc(txn->invalidations, sizeof(SharedInvalidationMessage) *
                     (txn->ninvalidations + nmsgs));

        memcpy(txn->invalidations + txn->ninvalidations, msgs,
               nmsgs * sizeof(SharedInvalidationMessage));
        txn->ninvalidations += nmsgs;
    }

    change = ReorderBufferGetChange(rb);
    change->action = REORDER_BUFFER_CHANGE_INVALIDATION;
    change->data.inval.ninvalidations = nmsgs;
    change->data.inval.invalidations = (SharedInvalidationMessage *)
        palloc(sizeof(SharedInvalidationMessage) * nmsgs);
    
    int rc = memcpy_s(change->data.inval.invalidations,
            sizeof(SharedInvalidationMessage) * nmsgs,
            msgs,
            sizeof(SharedInvalidationMessage) * nmsgs);
    securec_check(rc, "", "");

    ReorderBufferQueueChange(ctx, xid, lsn, change, false);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Apply all invalidations we know. Possibly we only need parts at this point
 * in the changestream but we don't know which those are.
 */
static void ReorderBufferExecuteInvalidations(uint32 nmsgs, SharedInvalidationMessage *msgs)
{
    for (uint32 i = 0; i < nmsgs; i++)
        LocalExecuteThreadAndSessionInvalidationMessage(&msgs[i]);
}

/*
 * Mark a transaction as containing catalog changes
 */
void ReorderBufferXidSetCatalogChanges(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
    ReorderBufferTXN *txn = NULL;

    /* When the thirdly parameter 'create' is true, the ret value must not be NULL. */
    txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);
    Assert(txn != NULL);
    txn->txn_flags |= RBTXN_HAS_CATALOG_CHANGES;

    /*
     * Mark top-level transaction as having catalog changes too if one of its
     * children has so that the ReorderBufferBuildTupleCidHash can
     * conveniently check just top-level transaction and decide whether to
     * build the hash table or not.
     */
    if (txn->toptxn != NULL)
        txn->toptxn->txn_flags |= RBTXN_HAS_CATALOG_CHANGES;
}

/*
 * Query whether a transaction is already *known* to contain catalog
 * changes. This can be wrong until directly before the commit!
 */
bool ReorderBufferXidHasCatalogChanges(ReorderBuffer *rb, TransactionId xid)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);
    if (txn == NULL)
        return false;

    return rbtxn_has_catalog_changes(txn);
}

/*
 * ReorderBufferXidHasBaseSnapshot
 *     Have we already set the base snapshot for the given txn/subtxn?
 */
bool ReorderBufferXidHasBaseSnapshot(ReorderBuffer *rb, TransactionId xid)
{
    ReorderBufferTXN *txn = NULL;

    txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr, false);
    /* transaction isn't known yet, ergo no snapshot */
    if (txn == NULL)
        return false;

    /* a known subtxn? operate on top-level txn instead */
    if (rbtxn_is_known_subxact(txn)) {
        txn = ReorderBufferTXNByXid(rb, txn->toplevel_xid, false, NULL, InvalidXLogRecPtr, false);
        if (txn == NULL) {
            return false;
        }
    }
    return txn->base_snapshot != NULL;
}

/*
 * ---------------------------------------
 * Disk serialization support
 * ---------------------------------------
 *
 *
 * Ensure the IO buffer is >= sz.
 */
static void ReorderBufferSerializeReserve(ReorderBuffer *rb, Size sz)
{
    if (!rb->outbufsize) {
        rb->outbuf = (char *)MemoryContextAlloc(rb->context, sz);
        rb->outbufsize = sz;
    } else if (rb->outbufsize < sz) {
        rb->outbuf = (char *)repalloc(rb->outbuf, sz);
        rb->outbufsize = sz;
    }
}

/* Compare two transactions by size */
static int ReorderBufferTXNSizeCompare(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    const ReorderBufferTXN *ta = pairingheap_const_container(ReorderBufferTXN, txn_node, a);
    const ReorderBufferTXN *tb = pairingheap_const_container(ReorderBufferTXN, txn_node, b);

    if (ta->size < tb->size)
        return -1;
    if (ta->size > tb->size)
        return 1;
    return 0;
}

/* Returns true, if the output plugin supports streaming, false, otherwise. */
static inline bool ReorderBufferCanStream(ReorderBuffer *rb) {
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)rb->private_data;
    return ctx->streaming;
}

/* Returns true, if the streaming can be started now, false, otherwise. */
static inline bool ReorderBufferCanStartStreaming(ReorderBuffer *rb)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)(rb->private_data);
    SnapBuild  *builder = ctx->snapshot_builder;

    /* We can't start streaming unless a consistent state is reached. */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_CONSISTENT)
        return false;

    /*
     * We can't start streaming immediately even if the streaming is enabled
     * because we previously decoded this transaction and now just are
     * restarting.
     */
    if (ReorderBufferCanStream(rb) &&
        !SnapBuildXactNeedsSkip(builder, ctx->reader->ReadRecPtr))
        return true;

    return false;
}

/*
 * Record the partial change for the streaming of in-progress transactions.  We
 * can stream only complete changes so if we have a partial change like toast
 * table insert or speculative insert then we mark such a 'txn' so that it
 * can't be streamed.  We also ensure that if the changes in such a 'txn' can
 * be streamed and are above logical_decoding_work_mem threshold then we stream
 * them as soon as we have a complete change.
 */
static void ReorderBufferProcessPartialChange(
    ReorderBuffer *rb, ReorderBufferTXN *txn, ReorderBufferChange *change, bool toast_insert)
{
    ReorderBufferTXN *toptxn;

    /*
     * The partial changes need to be processed only while streaming
     * in-progress transactions.
     */
    if (!ReorderBufferCanStream(rb))
        return;

    /* Get the top transaction. */
    toptxn = rbtxn_get_toptxn(txn);

    /*
     * Indicate a partial change for toast inserts.  The change will be
     * considered as complete once we get the insert or update on the main
     * table and we are sure that the pending toast chunks are not required
     * anymore.
     *
     * If we allow streaming when there are pending toast chunks then such
     * chunks won't be released till the insert (multi_insert) is complete and
     * we expect the txn to have streamed all changes after streaming.  This
     * restriction is mainly to ensure the correctness of streamed
     * transactions and it doesn't seem worth uplifting such a restriction
     * just to allow this case because anyway we will stream the transaction
     * once such an insert is complete.
     */
    if (toast_insert)
        toptxn->txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
    else if (rbtxn_has_partial_change(toptxn) &&
             IsInsertOrUpdate(change->action) &&
             change->data.tp.clear_toast_afterwards)
        toptxn->txn_flags &= ~RBTXN_HAS_PARTIAL_CHANGE;

    /*
     * Stream the transaction if it is serialized before and the changes are
     * now complete in the top-level transaction.
     *
     * The reason for doing the streaming of such a transaction as soon as we
     * get the complete change for it is that previously it would have reached
     * the memory threshold and wouldn't get streamed because of incomplete
     * changes.  Delaying such transactions would increase apply lag for them.
     */
    if (ReorderBufferCanStartStreaming(rb) &&
        !(rbtxn_has_partial_change(toptxn)) &&
        rbtxn_is_serialized(txn) &&
        rbtxn_has_streamable_change(toptxn))
        ReorderBufferStreamTXN(rb, toptxn);
}

/*
 * Find the largest transaction (toplevel or subxact) to evict (spill to disk).
 *
 * XXX With many subtransactions this might be quite slow, because we'll have
 * to walk through all of them. There are some options how we could improve
 * that: (a) maintain some secondary structure with transactions sorted by
 * amount of changes, (b) not looking for the entirely largest transaction,
 * but e.g. for transaction using at least some fraction of the memory limit,
 * and (c) evicting multiple transactions at once, e.g. to free a given portion
 * of the memory limit (e.g. 50%).
 */
static ReorderBufferTXN * ReorderBufferLargestTXN(ReorderBuffer *rb)
{
    ReorderBufferTXN *largest;

    /* Get the largest transaction from the max-heap */
    largest = pairingheap_container(ReorderBufferTXN, txn_node,
                                    pairingheap_first(rb->txn_heap));
    Assert(largest);
    Assert(largest->size > 0);
    Assert(largest->size <= rb->size);

    return largest;
}

/*
 * Find the largest streamable (and non-aborted) toplevel transaction to evict
 * (by streaming).
 *
 * This can be seen as an optimized version of ReorderBufferLargestTXN, which
 * should give us the same transaction (because we don't update memory account
 * for subtransaction with streaming, so it's always 0). But we can simply
 * iterate over the limited number of toplevel transactions that have a base
 * snapshot. There is no use of selecting a transaction that doesn't have base
 * snapshot because we don't decode such transactions.  Also, we do not select
 * the transaction which doesn't have any streamable change.
 *
 * Note that, we skip transactions that contains incomplete changes. There
 * is a scope of optimization here such that we can select the largest
 * transaction which has incomplete changes.  But that will make the code and
 * design quite complex and that might not be worth the benefit.  If we plan to
 * stream the transactions that contains incomplete changes then we need to
 * find a way to partially stream/truncate the transaction changes in-memory
 * and build a mechanism to partially truncate the spilled files.
 * Additionally, whenever we partially stream the transaction we need to
 * maintain the last streamed lsn and next time we need to restore from that
 * segment and the offset in WAL.  As we stream the changes from the top
 * transaction and restore them subtransaction wise, we need to even remember
 * the subxact from where we streamed the last change.
 */
static ReorderBufferTXN * ReorderBufferLargestStreamableTopTXN(ReorderBuffer *rb)
{
    dlist_iter  iter;
    Size        largest_size = 0;
    ReorderBufferTXN *largest = NULL;

    /* Find the largest top-level transaction having a base snapshot. */
    dlist_foreach(iter, &rb->txns_by_base_snapshot_lsn) {
        ReorderBufferTXN *txn;

        txn = dlist_container(ReorderBufferTXN, base_snapshot_node, iter.cur);

        /* must not be a subtxn */
        Assert(!rbtxn_is_known_subxact(txn));
        /* base_snapshot must be set */
        Assert(txn->base_snapshot != NULL);

        /* Don't consider these kinds of transactions for eviction. */
        if (rbtxn_has_partial_change(txn) ||
            !rbtxn_has_streamable_change(txn) ||
            rbtxn_is_aborted(txn))
            continue;

        if ((largest == NULL || txn->total_size > largest_size) &&
            (txn->total_size > 0)) {
            largest = txn;
            largest_size = txn->total_size;
        }
    }

    return largest;
}

/*
 * Check whether the logical_decoding_work_mem limit was reached, and if yes
 * pick the largest (sub)transaction at-a-time to evict and spill its changes to
 * disk until we reach under the memory limit.
 *
 * XXX At this point we select the transactions until we reach under the memory
 * limit, but we might also adapt a more elaborate eviction strategy - for example
 * evicting enough transactions to free certain fraction (e.g. 50%) of the memory
 * limit.
 */
static void ReorderBufferCheckMemoryLimit(ReorderBuffer *rb)
{
    ReorderBufferTXN *txn;
    Size logical_decoding_work_mem = u_sess->attr.attr_memory.logical_decoding_work_mem;

    /* bail out if we haven't exceeded the memory limit */
    if (rb->size < logical_decoding_work_mem * 1024L)
        return;

    /*
     * Loop until we reach under the memory limit.  One might think that just
     * by evicting the largest (sub)transaction we will come under the memory
     * limit based on assumption that the selected transaction is at least as
     * large as the most recent change (which caused us to go over the memory
     * limit). However, that is not true because a user can reduce the
     * logical_decoding_work_mem to a smaller value before the most recent
     * change.
     */
    while (rb->size >= logical_decoding_work_mem * 1024L) {
        /*
         * Pick the largest non-aborted transaction (or subtransaction) and evict
         * it from memory by streaming, if possible.  Otherwise, spill to disk.
         */
        if (ReorderBufferCanStartStreaming(rb) &&
            (txn = ReorderBufferLargestStreamableTopTXN(rb)) != NULL) {
            /* we know there has to be one, because the size is not zero */
            Assert(txn && !txn->toptxn);
            Assert(txn->total_size > 0);
            Assert(rb->size >= txn->total_size);

            /* skip the transaction if aborted */
            if (ReorderBufferCheckAndTruncateAbortedTXN(rb, txn))
                continue;

            ReorderBufferStreamTXN(rb, txn);
        } else {
            /*
             * Pick the largest transaction (or subtransaction) and evict it
             * from memory by serializing it to disk.
             */
            txn = ReorderBufferLargestTXN(rb);

            /* we know there has to be one, because the size is not zero */
            Assert(txn);
            Assert(txn->size > 0);
            Assert(rb->size >= txn->size);

            /* skip the transaction if aborted */
            if (ReorderBufferCheckAndTruncateAbortedTXN(rb, txn))
                continue;

            ReorderBufferSerializeTXN(rb, txn);
        }

        /*
         * After eviction, the transaction should have no entries in memory,
         * and should use 0 bytes for changes.
         */
        Assert(txn->size == 0);
        Assert(txn->nentries_mem == 0);
    }
    /* We must be under the memory limit now. */
    Assert(rb->size < logical_decoding_work_mem * 1024L);
}

/*
 * Check whether the transaction tx should spill its data to disk.
 */
static void ReorderBufferCheckSerializeTXN(LogicalDecodingContext *ctx,
    ReorderBufferTXN *txn, ReorderBufferChange *change, bool toast_insert)
{
    /* process partial change */
    ReorderBufferProcessPartialChange(ctx->reorder, txn, change, toast_insert);

    /* check the memory limits and evict something if needed */
    ReorderBufferCheckMemoryLimit(ctx->reorder);
}

/*
 * Spill data of a large transaction (and its subtransactions) to disk.
 */
static void ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    dlist_iter subtxn_i;
    dlist_mutable_iter change_i;
    int fd = -1;
    XLogSegNo curOpenSegNo = 0;

    Size spilled = 0;
    char path[MAXPGPATH];
    int nRet = 0;
    Size currentTxnSize = txn->size;
    Size currentRbSize = rb->size;

    ereport(DEBUG2, (errmodule(MOD_LOGICAL_DECODE),
        errmsg("spill %u changes in tx %lu to disk", (uint32)txn->nentries_mem, txn->xid)));

    /* do the same to all child TXs */
    if (&txn->subtxns == NULL) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("txn->subtxns is illegal point")));
    }
    dlist_foreach(subtxn_i, &txn->subtxns)
    {
        ReorderBufferTXN *subtxn = NULL;

        subtxn = dlist_container(ReorderBufferTXN, node, subtxn_i.cur);
        ReorderBufferSerializeTXN(rb, subtxn);
    }

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    /* serialize changestream */
    dlist_foreach_modify(change_i, &txn->changes)
    {
        ReorderBufferChange *change = NULL;

        change = dlist_container(ReorderBufferChange, node, change_i.cur);
        /*
         * store in segment in which it belongs by start lsn, don't split over
         * multiple segments tho
         */
        if (fd == -1 || !((change->lsn) / XLogSegSize == curOpenSegNo)) {
            XLogRecPtr recptr;

            if (fd != -1) {
                (void)CloseTransientFile(fd);
            }
            curOpenSegNo = (change->lsn) / XLogSegSize;

            recptr = (curOpenSegNo * XLogSegSize);

            /*
             * No need to care about TLIs here, only used during a single run,
             * so each LSN only maps to a specific WAL record.
             */
            nRet = sprintf_s(path, MAXPGPATH, "%s/%s/snap/xid-%lu-lsn-%X-%X.snap", replslot_path,
                             NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name), txn->xid, (uint32)(recptr >> 32),
                             (uint32)recptr);
            securec_check_ss(nRet, "", "");
            /* open segment, create it if necessary */
            fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_APPEND | PG_BINARY, S_IRUSR | S_IWUSR);
            if (fd < 0) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                    errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
            }
        }

        ReorderBufferSerializeChange(rb, txn, fd, change);
        dlist_delete(&change->node);
        ReorderBufferReturnChange(rb, change, false);

        spilled++;
    }

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, currentTxnSize);

    /* update the statistics if we have spilled anything */
    if (spilled) {
        rb->spillCount += 1;
        rb->spillBytes += currentTxnSize;
        /* don't consider already serialized transactions */
        rb->spillTxns += (rbtxn_is_serialized(txn) || rbtxn_is_serialized_clear(txn)) ? 0 : 1;
    }

    Assert(spilled == txn->nentries_mem);
    Assert(dlist_is_empty(&txn->changes));
    if (!rbtxn_is_serialized(txn)) {
        ereport(DEBUG5, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("ReorderBufferSerializeTXN for xid = %lu. "
            "Before serialization rb->size = %lu, txn->size = %lu. "
            "After serialization rb->size = %lu, txn->size = %lu.",
            txn->xid,
            currentRbSize, currentTxnSize, rb->size, txn->size)));
    }
    txn->txn_flags |= RBTXN_IS_SERIALIZED;
    txn->nentries_mem = 0;

    if (fd != -1) {
        (void)CloseTransientFile(fd);
    }
}

/*
 * Serialize individual change to disk.
 */
static void ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn, int fd, ReorderBufferChange *change)
{
    ReorderBufferDiskChange *ondisk = NULL;
    Size sz = sizeof(ReorderBufferDiskChange);
    int rc = 0;
    ReorderBufferSerializeReserve(rb, sz);

    ondisk = (ReorderBufferDiskChange *)rb->outbuf;
    rc = memcpy_s(&ondisk->change, sizeof(ReorderBufferChange), change, sizeof(ReorderBufferChange));
    securec_check(rc, "", "");

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_UINSERT:
        case REORDER_BUFFER_CHANGE_UDELETE:
        case REORDER_BUFFER_CHANGE_UUPDATE: {
            char *data = NULL;
            ReorderBufferTupleBuf *oldtup = NULL;
            ReorderBufferTupleBuf *newtup = NULL;
            Size oldlen = 0;
            Size newlen = 0;

            oldtup = change->data.tp.oldtuple;
            newtup = change->data.tp.newtuple;

            if (oldtup != NULL) {
                sz += sizeof(HeapTupleData);
                oldlen = oldtup->tuple.t_len;
                sz += oldlen;
            }

            if (newtup != NULL) {
                sz += sizeof(HeapTupleData);
                newlen = newtup->tuple.t_len;
                sz += newlen;
            }
#ifdef ENABLE_NEON
            if ((change->action == REORDER_BUFFER_CHANGE_INSERT ||
                 change->action == REORDER_BUFFER_CHANGE_UPDATE ||
                 change->action == REORDER_BUFFER_CHANGE_DELETE) &&
                change->data.tp.changed_attrs_valid &&
                change->data.tp.nchanged_attrs > 0) {
                sz += sizeof(AttrNumber) * change->data.tp.nchanged_attrs;
            }
#endif

            /* make sure we have enough space */
            ReorderBufferSerializeReserve(rb, sz);

            data = ((char *)rb->outbuf) + sizeof(ReorderBufferDiskChange);
            /* might have been reallocated above */
            ondisk = (ReorderBufferDiskChange *)rb->outbuf;

#ifdef ENABLE_NEON
            if ((change->action == REORDER_BUFFER_CHANGE_INSERT ||
                 change->action == REORDER_BUFFER_CHANGE_UPDATE ||
                 change->action == REORDER_BUFFER_CHANGE_DELETE) &&
                change->data.tp.changed_attrs_valid &&
                change->data.tp.nchanged_attrs > 0) {
                Size changed_attrs_size = sizeof(AttrNumber) * change->data.tp.nchanged_attrs;

                rc = memcpy_s(data, changed_attrs_size, change->data.tp.changed_attrs, changed_attrs_size);
                securec_check(rc, "", "");
                data += changed_attrs_size;
            }
#endif

            if (oldlen) {
                rc = memcpy_s(data, sizeof(HeapTupleData), &oldtup->tuple, sizeof(HeapTupleData));
                securec_check(rc, "", "");
                data += sizeof(HeapTupleData);
                rc = memcpy_s(data, oldlen, oldtup->tuple.t_data, oldlen);
                securec_check(rc, "", "");
                data += oldlen;
            }

            if (newlen) {
                rc = memcpy_s(data, sizeof(HeapTupleData), &newtup->tuple, sizeof(HeapTupleData));
                securec_check(rc, "", "");

                data += sizeof(HeapTupleData);
                rc = memcpy_s(data, newlen, newtup->tuple.t_data, newlen);
                securec_check(rc, "", "");
                data += oldlen;
            }
            break;
        }
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT: {
            Snapshot snap = NULL;
            char *data = NULL;

            snap = change->data.snapshot;

            sz += sizeof(SnapshotData) + sizeof(TransactionId) * snap->xcnt + sizeof(TransactionId) * snap->subxcnt;

            /* make sure we have enough space */
            ReorderBufferSerializeReserve(rb, sz);
            data = ((char *)rb->outbuf) + sizeof(ReorderBufferDiskChange);
            /* might have been reallocated above */
            ondisk = (ReorderBufferDiskChange *)rb->outbuf;

            rc = memcpy_s(data, sizeof(SnapshotData), snap, sizeof(SnapshotData));
            securec_check(rc, "", "");
            data += sizeof(SnapshotData);

            if (snap->xcnt) {
                rc = memcpy_s(data, sizeof(TransactionId) * snap->xcnt, snap->xip, sizeof(TransactionId) * snap->xcnt);
                securec_check(rc, "", "");
                data += sizeof(TransactionId) * snap->xcnt;
            }

            if (snap->subxcnt) {
                rc = memcpy_s(data, sizeof(TransactionId) * snap->subxcnt, snap->subxip,
                              sizeof(TransactionId) * snap->subxcnt);
                securec_check(rc, "", "");
                data += sizeof(TransactionId) * snap->subxcnt;
            }
            break;
        }
        case REORDER_BUFFER_CHANGE_INVALIDATION: {
            char *data;
            Size  inval_size = sizeof(SharedInvalidationMessage) * change->data.inval.ninvalidations;

            sz += inval_size;

            ReorderBufferSerializeReserve(rb, sz);
            data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);

            /* might have been reallocated above */
            ondisk = (ReorderBufferDiskChange *) rb->outbuf;
            rc = memcpy_s(data, inval_size, change->data.inval.invalidations, inval_size);
            securec_check(rc, "", "");
            data += inval_size;

            break;
        }
        case REORDER_BUFFER_CHANGE_DDL: {
            char       *data;
            Size        prefix_size = strlen(change->data.ddl.prefix) + 1;

            sz += prefix_size + change->data.ddl.message_size +
                sizeof(Size) + sizeof(Oid) + sizeof(DeparsedCommandType) + sizeof(Size);
            ReorderBufferSerializeReserve(rb, sz);

            data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);

            /* might have been reallocated above */
            ondisk = (ReorderBufferDiskChange *) rb->outbuf;

            /* write the prefix, relid and cmdtype including the size */
            rc = memcpy_s(data, sizeof(Size), &prefix_size, sizeof(Size));
            securec_check(rc, "", "");
            data += sizeof(Size);
            rc = memcpy_s(data, sizeof(Oid), &change->data.ddl.relid, sizeof(Oid));
            securec_check(rc, "", "");
            data += sizeof(Oid);
            rc = memcpy_s(data, sizeof(DeparsedCommandType), &change->data.ddl.cmdtype, sizeof(DeparsedCommandType));
            securec_check(rc, "", "");
            data += sizeof(DeparsedCommandType);
            rc = memcpy_s(data, prefix_size, change->data.ddl.prefix, prefix_size);
            securec_check(rc, "", "");
            data += prefix_size;

            /* write the message including the size */
            rc = memcpy_s(data, sizeof(Size), &change->data.ddl.message_size, sizeof(Size));
            securec_check(rc, "", "");
            data += sizeof(Size);
            rc = memcpy_s(data, change->data.ddl.message_size, change->data.ddl.message,
                    change->data.ddl.message_size);
            securec_check(rc, "", "");
            data += change->data.ddl.message_size;

            break;
        }
        case REORDER_BUFFER_CHANGE_TRUNCATE:
        case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
            /* ReorderBufferChange contains everything important */
            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
            /* ReorderBufferChange contains everything important */
            break;
    }

    ondisk->size = sz;

    if ((Size)(write(fd, rb->outbuf, ondisk->size)) != ondisk->size) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
            errcode_for_file_access(), errmsg("could not write to xid %lu's data file: %s", txn->xid, TRANSLATE_ERRNO)));
    }

    if (txn->final_lsn < change->lsn) {
        txn->final_lsn = change->lsn;
    }
    Assert(ondisk->change.action == change->action);
}

/*
 * Restore a number of changes spilled to disk back into memory.
 */
static Size ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn, int *fd, XLogSegNo *segno)
{
    Size restored = 0;
    XLogSegNo last_segno;
    int rc = 0;
    dlist_mutable_iter cleanup_iter;

    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));
    Assert(!XLByteEQ(txn->final_lsn, InvalidXLogRecPtr));

    /* free current entries, so we have memory for more */
    dlist_foreach_modify(cleanup_iter, &txn->changes)
    {
        ReorderBufferChange *cleanup = dlist_container(ReorderBufferChange, node, cleanup_iter.cur);

        dlist_delete(&cleanup->node);
        ReorderBufferReturnChange(rb, cleanup);
    }
    txn->nentries_mem = 0;
    Assert(dlist_is_empty(&txn->changes));

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    last_segno = (txn->final_lsn) / XLogSegSize;
    while (restored < (unsigned)g_instance.attr.attr_common.max_changes_in_memory && *segno <= last_segno) {
        int readBytes;
        ReorderBufferDiskChange *ondisk = NULL;
        CHECK_FOR_INTERRUPTS();

        if (*fd == -1) {
            XLogRecPtr recptr;
            char path[MAXPGPATH];

            /* first time in */
            if (*segno == 0) {
                *segno = (txn->first_lsn) / XLogSegSize;
            }

            Assert(*segno != 0 || dlist_is_empty(&txn->changes));

            recptr = (*segno * XLogSegSize);

            /*
             * No need to care about TLIs here, only used during a single run,
             * so each LSN only maps to a specific WAL record.
             */
            rc = sprintf_s(path, sizeof(path), "%s/%s/snap/xid-%lu-lsn-%X-%X.snap", replslot_path,
                           NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name), txn->xid, (uint32)(recptr >> 32),
                           (uint32)recptr);
            securec_check_ss(rc, "", "");
            *fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
            if (*fd < 0 && errno == ENOENT) {
                *fd = -1;
                (*segno)++;
                continue;
            } else if (*fd < 0)
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                    errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }

        /*
         * Read the statically sized part of a change which has information
         * about the total size. If we couldn't read a record, we're at the
         * end of this file.
         */
        ReorderBufferSerializeReserve(rb, sizeof(ReorderBufferDiskChange));
        readBytes = read(*fd, rb->outbuf, sizeof(ReorderBufferDiskChange));
        /* eof */
        if (readBytes == 0) {
            (void)CloseTransientFile(*fd);
            *fd = -1;
            (*segno)++;
            continue;
        } else if (readBytes < 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                errcode_for_file_access(), errmsg("could not read from reorderbuffer spill file: %m")));
        } else if (readBytes != sizeof(ReorderBufferDiskChange)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("incomplete read from reorderbuffer spill file: read %d instead of %u",
                       readBytes, (uint32)sizeof(ReorderBufferDiskChange))));
        }

        ondisk = (ReorderBufferDiskChange *)rb->outbuf;

        ReorderBufferSerializeReserve(rb, sizeof(ReorderBufferDiskChange) + ondisk->size);
        ondisk = (ReorderBufferDiskChange *)rb->outbuf;
        if (ondisk->size < sizeof(ReorderBufferDiskChange)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("illegality read length %lu", (ondisk->size - sizeof(ReorderBufferDiskChange)))));
        }
        readBytes = read(*fd, rb->outbuf + sizeof(ReorderBufferDiskChange),
                         uint64(ondisk->size) - sizeof(ReorderBufferDiskChange));
        if (readBytes < 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("could not read from reorderbuffer spill file: %m")));
        } else if (INT2SIZET(readBytes) != ondisk->size - sizeof(ReorderBufferDiskChange)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("could not read from reorderbuffer spill file: read %d instead of %u",
                       readBytes, (uint32)(ondisk->size - sizeof(ReorderBufferDiskChange)))));
        }

        /*
         * ok, read a full change from disk, now restore it into proper
         * in-memory format
         */
        ReorderBufferRestoreChange(rb, txn, rb->outbuf);
        restored++;
    }

    return restored;
}

/*
 * Convert change from its on-disk format to in-memory format and queue it onto
 * the TXN's ->changes list.
 */
static void ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn, char *data)
{
    ReorderBufferDiskChange *ondisk = NULL;
    ReorderBufferChange *change = NULL;
    int rc = 0;
    ondisk = (ReorderBufferDiskChange *)data;

    change = ReorderBufferGetChange(rb);

    /* copy static part */
    rc = memcpy_s(change, sizeof(ReorderBufferChange), &ondisk->change, sizeof(ReorderBufferChange));
    securec_check(rc, "", "");

    data += sizeof(ReorderBufferDiskChange);

    /* restore individual stuff */
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        /* fall through */
        case REORDER_BUFFER_CHANGE_UPDATE:
        /* fall through */
        case REORDER_BUFFER_CHANGE_DELETE:
#ifdef ENABLE_NEON
            if (change->data.tp.changed_attrs_valid && change->data.tp.nchanged_attrs > 0) {
                Size changed_attrs_size = sizeof(AttrNumber) * change->data.tp.nchanged_attrs;

                change->data.tp.changed_attrs = (AttrNumber *)MemoryContextAlloc(rb->context, changed_attrs_size);
                rc = memcpy_s(change->data.tp.changed_attrs, changed_attrs_size, data, changed_attrs_size);
                securec_check(rc, "", "");
                data += changed_attrs_size;
            } else {
                change->data.tp.changed_attrs = NULL;
            }
#endif
            if (change->data.tp.oldtuple) {
                Size tuplelen = ((HeapTuple)data)->t_len;
                change->data.tp.oldtuple = ReorderBufferGetTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);

                /* restore ->tuple */
                rc = memcpy_s(&change->data.tp.oldtuple->tuple, sizeof(HeapTupleData), data, sizeof(HeapTupleData));
                securec_check(rc, "", "");
                data += sizeof(HeapTupleData);
                change->data.tp.oldtuple->tuple.t_data = ReorderBufferTupleBufData(change->data.tp.oldtuple);
                /* restore tuple data itself */
                rc = memcpy_s(change->data.tp.oldtuple->tuple.t_data, tuplelen, data, tuplelen);
                securec_check(rc, "", "");
                data += tuplelen;
            }
            if (change->data.tp.newtuple) {
                Size tuplelen = ((HeapTuple)data)->t_len;
                change->data.tp.newtuple = ReorderBufferGetTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);

                /* restore ->tuple */
                rc = memcpy_s(&change->data.tp.newtuple->tuple, sizeof(HeapTupleData), data, sizeof(HeapTupleData));
                securec_check(rc, "", "");
                data += sizeof(HeapTupleData);

                /* reset t_data pointer into the new tuplebuf */
                change->data.tp.newtuple->tuple.t_data = ReorderBufferTupleBufData(change->data.tp.newtuple);

                /* restore tuple data itself */
                rc = memcpy_s(change->data.tp.newtuple->tuple.t_data, tuplelen, data, tuplelen);
                securec_check(rc, "", "");
                data += tuplelen;
            }

            break;
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT: {
            Snapshot oldsnap;
            Snapshot newsnap;
            Size size;

            oldsnap = (Snapshot)data;

            size = sizeof(SnapshotData) + sizeof(TransactionId) * oldsnap->xcnt +
                   sizeof(TransactionId) * (oldsnap->subxcnt + 0);

            change->data.snapshot = (SnapshotData *)MemoryContextAllocZero(rb->context, size);

            newsnap = change->data.snapshot;

            rc = memcpy_s(newsnap, size, data, size);
            securec_check(rc, "", "");
            newsnap->xip = (TransactionId *)(((char *)newsnap) + sizeof(SnapshotData));
            newsnap->subxip = newsnap->xip + newsnap->xcnt;
            newsnap->copied = true;
            break;
        }
        case REORDER_BUFFER_CHANGE_INVALIDATION: {
            Size inval_size = sizeof(SharedInvalidationMessage) * change->data.inval.ninvalidations;

            change->data.inval.invalidations = (SharedInvalidationMessage*)MemoryContextAlloc(rb->context, inval_size);

            /* read the message */
            rc = memcpy_s(change->data.inval.invalidations, inval_size, data, inval_size);
            securec_check(rc, "", "");

            break;
        }
        /* the base struct contains all the data, easy peasy */
        case REORDER_BUFFER_CHANGE_TRUNCATE:
        case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
        case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
            break;
        case REORDER_BUFFER_CHANGE_DDL: {
            Size prefix_size;

            /* read prefix */
            rc = memcpy_s(&prefix_size, sizeof(Size), data, sizeof(Size));
            securec_check(rc, "", "");
            data += sizeof(Size);
            rc = memcpy_s(&change->data.ddl.relid, sizeof(Oid), data, sizeof(Oid));
            securec_check(rc, "", "");
            data += sizeof(Oid);
            rc = memcpy_s(&change->data.ddl.cmdtype, sizeof(DeparsedCommandType), data, sizeof(DeparsedCommandType));
            securec_check(rc, "", "");
            data += sizeof(DeparsedCommandType);

            change->data.ddl.prefix = (char*)MemoryContextAlloc(rb->context, prefix_size);
            rc = memcpy_s(change->data.ddl.prefix, prefix_size, data, prefix_size);
            securec_check(rc, "", "");
            Assert(change->data.ddl.prefix[prefix_size - 1] == '\0');
            data += prefix_size;

            /* read the message */
            rc = memcpy_s(&change->data.ddl.message_size, sizeof(Size), data, sizeof(Size));
            securec_check(rc, "", "");
            data += sizeof(Size);
            change->data.ddl.message = (char*)MemoryContextAlloc(rb->context,
                                                            change->data.ddl.message_size);
            rc = memcpy_s(change->data.ddl.message, change->data.ddl.message_size, data,
                    change->data.ddl.message_size);
            securec_check(rc, "", "");
            data += change->data.ddl.message_size;

            break;
        }
        case REORDER_BUFFER_CHANGE_UINSERT:
        case REORDER_BUFFER_CHANGE_UDELETE:
        case REORDER_BUFFER_CHANGE_UUPDATE:
            if (change->data.utp.oldtuple) {
                Size tuplelen = ((UHeapTuple)data)->disk_tuple_size;
                change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(rb, tuplelen - SizeOfUHeapHeader);

                /* restore ->tuple */
                rc = memcpy_s(&change->data.utp.oldtuple->tuple, sizeof(UHeapTupleData), data, sizeof(UHeapTupleData));
                securec_check(rc, "", "");
                data += sizeof(UHeapTupleData);
                change->data.utp.oldtuple->tuple.disk_tuple = ReorderBufferUTupleBufData(change->data.utp.oldtuple);
                /* restore tuple data itself */
                rc = memcpy_s(change->data.utp.oldtuple->tuple.disk_tuple, tuplelen, data, tuplelen);
                securec_check(rc, "", "");
                data += tuplelen;
            }
            if (change->data.utp.newtuple) {
                Size tuplelen = ((UHeapTuple)data)->disk_tuple_size;
                change->data.utp.newtuple = ReorderBufferGetUTupleBuf(rb, tuplelen - SizeOfUHeapHeader);

                /* restore ->tuple */
                rc = memcpy_s(&change->data.utp.newtuple->tuple, sizeof(UHeapTupleData), data, sizeof(UHeapTupleData));
                securec_check(rc, "", "");
                data += sizeof(UHeapTupleData);

                /* reset t_data pointer into the new tuplebuf */
                change->data.utp.newtuple->tuple.disk_tuple = ReorderBufferUTupleBufData(change->data.utp.newtuple);

                /* restore tuple data itself */
                rc = memcpy_s(change->data.utp.newtuple->tuple.disk_tuple, tuplelen, data, tuplelen);
                securec_check(rc, "", "");
                data += tuplelen;
            }
            break;
    }

    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries_mem++;
    ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, ReorderBufferChangeSize(change));
}

/*
 * Remove all on-disk stored for the passed in transaction.
 */
static void ReorderBufferRestoreCleanup(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr lsn = InvalidXLogRecPtr)
{
    XLogSegNo first;
    XLogSegNo cur;
    XLogSegNo last;
    ReplicationSlot *slot = NULL;
    int rc = 0;
    if (txn->final_lsn == InvalidXLogRecPtr) {
        txn->final_lsn = lsn;
    }
    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));

    first = (txn->first_lsn) / XLogSegSize;
    last = (txn->final_lsn) / XLogSegSize;
    if (t_thrd.slot_cxt.MyReplicationSlot) {
        slot = t_thrd.slot_cxt.MyReplicationSlot;
    } else {
        slot = ((LogicalDecodingContext *)rb->private_data)->slot;
    }
    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    /* iterate over all possible filenames, and delete them */
    for (cur = first; cur <= last; cur++) {
        char path[MAXPGPATH];
        XLogRecPtr recptr;
        recptr = (cur * XLogSegSize);
        rc = sprintf_s(path, sizeof(path), "%s/%s/snap/xid-%lu-lsn-%X-%X.snap", replslot_path, NameStr(slot->data.name),
            txn->xid, (uint32)(recptr >> 32), uint32(recptr));
        securec_check_ss(rc, "", "");
        if (unlink(path) != 0 && errno != ENOENT)
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));
    }
}

/*
 * Remove all leftover reorderbuffer tmpfiles after a crash or the end of logical deocoding.
 */
void ReorderBufferClear(const char *slotname)
{
    struct dirent *spill_de = NULL;
    DIR *spill_dir = NULL;
    struct stat statbuf;
    char path[MAXPGPATH] = {0};
    char path_xid[MAXPGPATH] = {0};
    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    errno_t rc = sprintf_s(path, sizeof(path), "%s/%s/snap", replslot_path, slotname);
    securec_check_ss(rc, "", "");

    if (lstat(path, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
        return;
    }

    spill_dir = AllocateDir(path);
    while ((spill_de = ReadDirExtended(spill_dir, path, DEBUG2)) != NULL) {
        /* only look at names that can be ours */
        if (strncmp(spill_de->d_name, "xid", strlen("xid")) == 0) {
            rc = sprintf_s(path_xid, sizeof(path_xid), "%s/%s/snap/%s", replslot_path, slotname, spill_de->d_name);
            securec_check_ss(rc, "", "");
            if (unlink(path_xid) != 0) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE),
                    errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path_xid)));
            }
        }
    }
    if (spill_dir != NULL) {
        (void)FreeDir(spill_dir);
    }
}

/*
 * Delete all data spilled to disk after we've restarted/crashed. It will be
 * recreated when the respective slots are reused.
 */
void StartupReorderBuffer(void)
{
    DIR *logical_dir = NULL;
    struct dirent *logical_de = NULL;

    char replslot_path[MAXPGPATH];
    GetReplslotPath(replslot_path);
    logical_dir = AllocateDir(replslot_path);
    while ((logical_de = ReadDirExtended(logical_dir, replslot_path, DEBUG2)) != NULL) {
        if (strncmp(logical_de->d_name, ".", strlen(".")) == 0 ||
            strncmp(logical_de->d_name, "..", strlen("..")) == 0)
            continue;

        /* if it cannot be a slot, skip the directory */
        if (!ReplicationSlotValidateName(logical_de->d_name, DEBUG2))
            continue;

        /*
         * ok, has to be a surviving logical slot, iterate and delete
         * everythign starting with xid-*
         */
        ReorderBufferClear(logical_de->d_name);
    }
    if (logical_dir != NULL) {
        (void)FreeDir(logical_dir);
    }
}

/* ---------------------------------------
 * toast reassembly support
 * ---------------------------------------
 *
 *
 * Initialize per tuple toast reconstruction support.
 */
static void ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    HASHCTL hash_ctl;
    int rc = 0;
    Assert(txn->toast_hash == NULL);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");

    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ReorderBufferToastEnt);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = rb->context;
    txn->toast_hash = hash_create("ReorderBufferToastHash", 5, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * Per toast-chunk handling for toast reconstruction
 *
 * Appends a toast chunk so we can reconstruct it when the tuple "owning" the
 * toasted Datum comes along.
 */
static void ReorderBufferToastAppendChunk(ReorderBuffer *rb, ReorderBufferTXN *txn, Relation relation,
                                          ReorderBufferChange *change, bool isUHeap)
{
    ReorderBufferToastEnt *ent = NULL;
    ReorderBufferTupleBuf *newtup = NULL;
    bool found = false;
    int32 chunksize = 0;
    bool isnull = false;
    Pointer chunk = NULL;
    TupleDesc desc = RelationGetDescr(relation);
    Oid chunk_id = InvalidOid;
    Oid chunk_seq = InvalidOid;
    const int chunkIdIndex = 1;
    const int chunkSeqIndex = 2;
    const int chunkDataIndex = 3;

    if (txn->toast_hash == NULL) {
        ReorderBufferToastInitHash(rb, txn);
    }

    Assert(IsToastRelation(relation));

    newtup = isUHeap ? (ReorderBufferTupleBuf *)change->data.utp.newtuple : change->data.tp.newtuple;
    chunk_id = DatumGetObjectId(isUHeap ?
        UHeapFastGetAttr(&((ReorderBufferUTupleBuf *)newtup)->tuple, chunkIdIndex, desc, &isnull) :
        fastgetattr(&newtup->tuple, chunkIdIndex, desc, &isnull));
    Assert(!isnull);
    chunk_seq = DatumGetInt32(isUHeap ?
        UHeapFastGetAttr(&((ReorderBufferUTupleBuf *)newtup)->tuple, chunkSeqIndex, desc, &isnull) :
        fastgetattr(&newtup->tuple, chunkSeqIndex, desc, &isnull));
    Assert(!isnull);

    ent = (ReorderBufferToastEnt *)hash_search(txn->toast_hash, (void *)&chunk_id, HASH_ENTER, &found);

    if (!found) {
        Assert(ent->chunk_id == chunk_id);
        ent->num_chunks = 0;
        ent->last_chunk_seq = 0;
        ent->size = 0;
        ent->reconstructed = NULL;
        dlist_init(&ent->chunks);

        if (chunk_seq != 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("got sequence entry %u for toast chunk %u instead of seq 0", chunk_seq, chunk_id)));
        }
    } else if (found && chunk_seq != (Oid)ent->last_chunk_seq + 1) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("got sequence entry %u for toast chunk %u instead of seq %d",
                   chunk_seq, chunk_id, ent->last_chunk_seq + 1)));
    }

    chunk = DatumGetPointer(isUHeap ?
        UHeapFastGetAttr(&((ReorderBufferUTupleBuf *)newtup)->tuple, chunkDataIndex, desc, &isnull) :
        fastgetattr(&newtup->tuple, chunkDataIndex, desc, &isnull));
    Assert(!isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("fail to get toast chunk")));
    }
    checkHugeToastPointer((varlena *)chunk);
    /* calculate size so we can allocate the right size at once later */
    if (!VARATT_IS_EXTENDED(chunk)) {
        chunksize = VARSIZE(chunk) - VARHDRSZ;
    } else if (VARATT_IS_SHORT(chunk)) {
        /* could happen due to heap_form_tuple doing its thing */
        chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
    } else {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unexpected type of toast chunk")));
    }

    ent->size += chunksize;
    ent->last_chunk_seq = chunk_seq;
    ent->num_chunks++;
    dlist_push_tail(&ent->chunks, &change->node);
}

/*
 * Function to deform an attribute in a toast tuple.
 */
static void ReorderBufferFillAttributes(ReorderBufferTXN *txn, TupleDesc desc, TupleDesc toast_desc, bool isUHeap,
    Datum *attrs, bool *isnull, bool* free)
{
    errno_t rc = 0;
    const int toast_index = 3; /* toast index in tuple is 3 */
    for (int natt = 0; natt < desc->natts; natt++) {
        Form_pg_attribute attr = &desc->attrs[natt];
        ReorderBufferToastEnt *ent = NULL;
        struct varlena *varlena = NULL;

        /* va_rawsize is the size of the original datum -- including header */
        struct varatt_external toast_pointer;
        struct varatt_indirect redirect_pointer;
        struct varlena *new_datum = NULL;
        struct varlena *reconstructed = NULL;
        dlist_iter it;
        Size data_done = 0;
        int2 bucketid;

        /* system columns aren't toasted */
        if (attr->attnum < 0)
            continue;

        if (attr->attisdropped)
            continue;

        /* not a varlena datatype */
        if (attr->attlen != -1)
            continue;

        /* no data */
        if (isnull[natt])
            continue;

        /* ok, we know we have a toast datum */
        varlena = (struct varlena *)DatumGetPointer(attrs[natt]);
        /* no need to do anything if the tuple isn't external */
        if (!VARATT_IS_EXTERNAL(varlena))
            continue;

        VARATT_EXTERNAL_GET_POINTER_B(toast_pointer, varlena, bucketid);

        /*
         * Check whether the toast tuple changed, replace if so.
         */
        ent = (ReorderBufferToastEnt *)hash_search(txn->toast_hash, (void *)&toast_pointer.va_valueid, HASH_FIND, NULL);
        if (ent == NULL)
            continue;
        new_datum = (struct varlena *)palloc0(INDIRECT_POINTER_SIZE);
        free[natt] = true;
        reconstructed = (struct varlena *)palloc0(toast_pointer.va_rawsize);
        ent->reconstructed = reconstructed;

        /* stitch toast tuple back together from its parts */
        dlist_foreach(it, &ent->chunks)
        {
            bool isnul = false;
            ReorderBufferChange *cchange = NULL;
            ReorderBufferTupleBuf *ctup = NULL;
            Pointer chunk = NULL;

            cchange = dlist_container(ReorderBufferChange, node, it.cur);
            ctup = isUHeap ? (ReorderBufferTupleBuf *)cchange->data.utp.newtuple : cchange->data.tp.newtuple;
            chunk = DatumGetPointer(isUHeap ?
                UHeapFastGetAttr(&((ReorderBufferUTupleBuf *)ctup)->tuple, toast_index, toast_desc, &isnul):
                fastgetattr(&ctup->tuple, toast_index, toast_desc, &isnul));
            Assert(!isnul);
            Assert(!VARATT_IS_EXTERNAL(chunk));
            Assert(!VARATT_IS_SHORT(chunk));
            if (chunk == NULL)
                continue;
            rc = memcpy_s(VARDATA(reconstructed) + data_done, VARSIZE(chunk) - VARHDRSZ, VARDATA(chunk),
                          VARSIZE(chunk) - VARHDRSZ);
            securec_check(rc, "", "");
            data_done += VARSIZE(chunk) - VARHDRSZ;
        }
        Assert(data_done == (Size)toast_pointer.va_extsize);

        /* make sure its marked as compressed or not */
        if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
            SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
        else
            SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

        rc = memset_s(&redirect_pointer, sizeof(redirect_pointer), 0, sizeof(redirect_pointer));
        securec_check(rc, "", "");
        redirect_pointer.pointer = reconstructed;

        SET_VARTAG_EXTERNAL(new_datum, VARTAG_INDIRECT);
        rc = memcpy_s(VARDATA_EXTERNAL(new_datum), sizeof(redirect_pointer), &redirect_pointer,
                      sizeof(redirect_pointer));
        securec_check(rc, "", "");
        attrs[natt] = PointerGetDatum(new_datum);
    }
}

/*
 * Rejigger change->newtuple to point to in-memory toast tuples instead to
 * on-disk toast tuples that may not longer exist (think DROP TABLE or VACUUM).
 *
 * We cannot replace unchanged toast tuples though, so those will still point
 * to on-disk toast data.
 */
static void ReorderBufferToastReplace(ReorderBuffer *rb, ReorderBufferTXN *txn, Relation relation,
    ReorderBufferChange *change, Oid partationReltoastrelid, bool isUHeap)
{
    HeapTuple tmphtup = NULL;
    Relation toast_rel = NULL;
    TupleDesc toast_desc = NULL;
    ReorderBufferTupleBuf *newtup = NULL;
    Size old_size;
    /* no toast tuples changed */
    if (txn->toast_hash == NULL)
        return;

    /*
     * We're going to modify the size of the change. So, to make sure the
     * accounting is correct we record the current change size and then after
     * re-computing the change we'll subtract the recorded size and then
     * re-add the new change size at the end. We don't immediately subtract
     * the old size because if there is any error before we add the new size,
     * we will release the changes and that will update the accounting info
     * (subtracting the size from the counters). And we don't want to
     * underflow there.
     */
    old_size = ReorderBufferChangeSize(change);

    MemoryContext oldcontext = MemoryContextSwitchTo(rb->context);

    /* we should only have toast tuples in an INSERT or UPDATE */
    if (isUHeap) {
        Assert(change->data.utp.newtuple);
    } else {
        Assert(change->data.tp.newtuple);
    }

    TupleDesc desc = RelationGetDescr(relation);
    if (relation->rd_rel->reltoastrelid != InvalidOid)
        toast_rel = RelationIdGetRelation(relation->rd_rel->reltoastrelid);
    else
        toast_rel = RelationIdGetRelation(partationReltoastrelid);
    if (toast_rel == NULL) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
            errmsg("toast_rel should not be NULL!")));
    }
    toast_desc = RelationGetDescr(toast_rel);
    /* should we allocate from stack instead? */
    Datum *attrs = (Datum *)palloc0(sizeof(Datum) * desc->natts);
    bool *isnull = (bool *)palloc0(sizeof(bool) * desc->natts);
    bool *free = (bool *)palloc0(sizeof(bool) * desc->natts);
    if (isUHeap) {
        newtup = (ReorderBufferTupleBuf *)change->data.utp.newtuple;
        UHeapDeformTuple(&((ReorderBufferUTupleBuf *)newtup)->tuple, desc, attrs, isnull);
    } else {
        newtup = change->data.tp.newtuple;
        heap_deform_tuple(&newtup->tuple, desc, attrs, isnull);
    }
    ReorderBufferFillAttributes(txn, desc, toast_desc, isUHeap, attrs, isnull, free);
    /*
     * Build tuple in separate memory & copy tuple back into the tuplebuf
     * passed to the output plugin. We can't directly heap_fill_tuple() into
     * the tuplebuf because attrs[] will point back into the current content.
     */

    if (isUHeap) {
        tmphtup = (HeapTuple)UHeapFormTuple(desc, attrs, isnull);
        Assert(((ReorderBufferUTupleBuf *)newtup)->tuple.disk_tuple_size <= MaxPossibleUHeapTupleSize);
        Assert(ReorderBufferUTupleBufData(newtup) == ((ReorderBufferUTupleBuf *)newtup)->tuple.disk_tuple);
        errno_t rc = memcpy_s(((ReorderBufferUTupleBuf *)newtup)->tuple.disk_tuple,
            ((ReorderBufferUTupleBuf *)newtup)->alloc_tuple_size, ((UHeapTuple)tmphtup)->disk_tuple,
            ((UHeapTuple)tmphtup)->disk_tuple_size);
        securec_check(rc, "", "");
        ((ReorderBufferUTupleBuf *)newtup)->tuple.disk_tuple_size = ((UHeapTuple)tmphtup)->disk_tuple_size;
    } else {
        tmphtup = heap_form_tuple(desc, attrs, isnull);
        Assert(newtup->tuple.t_len <= MaxHeapTupleSize);
        Assert(ReorderBufferTupleBufData(newtup) == newtup->tuple.t_data);
        errno_t rc = memcpy_s(newtup->tuple.t_data, newtup->alloc_tuple_size, tmphtup->t_data, tmphtup->t_len);
        securec_check(rc, "", "");
        newtup->tuple.t_len = tmphtup->t_len;
    }
    /*
     * free resources we won't further need, more persistent stuff will be
     * free'd in ReorderBufferToastReset().
     */
    RelationClose(toast_rel);
    pfree_ext(tmphtup);
 
    for (int natt = 0; natt < desc->natts; natt++) {
        if (free[natt]) {
            pfree(DatumGetPointer(attrs[natt]));
        }
    }
    pfree_ext(attrs);
    pfree_ext(free);
    pfree_ext(isnull);
    (void)MemoryContextSwitchTo(oldcontext);

    /* subtract the old change size */
    ReorderBufferChangeMemoryUpdate(rb, change, NULL, false, old_size);
    /* now add the change back, with the correct size */
    ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, ReorderBufferChangeSize(change));
}

/*
 * Free all resources allocated for toast reconstruction.
 */
static void ReorderBufferToastReset(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    HASH_SEQ_STATUS hstat;
    ReorderBufferToastEnt *ent = NULL;

    if (txn->toast_hash == NULL) {
        return;
    }

    /* sequentially walk over the hash and free everything */
    hash_seq_init(&hstat, txn->toast_hash);
    while ((ent = (ReorderBufferToastEnt *)hash_seq_search(&hstat)) != NULL) {
        dlist_mutable_iter it;

        if (ent->reconstructed != NULL) {
            pfree(ent->reconstructed);
            ent->reconstructed = NULL;
        }

        dlist_foreach_modify(it, &ent->chunks)
        {
            ReorderBufferChange *change = dlist_container(ReorderBufferChange, node, it.cur);

            dlist_delete(&change->node);
            ReorderBufferReturnChange(rb, change);
        }
    }

    hash_destroy(txn->toast_hash);
    txn->toast_hash = NULL;
}

/* ---------------------------------------
 * Visibility support for logical decoding
 *
 *
 * Lookup actual cmin/cmax values when using decoding snapshot. We can't
 * always rely on stored cmin/cmax values because of two scenarios:
 *
 * * A tuple got changed multiple times during a single transaction and thus
 *   has got a combocid. Combocid's are only valid for the duration of a
 *   single transaction.
 * * A tuple with a cmin but no cmax (and thus no combocid) got
 *   deleted/updated in another transaction than the one which created it
 *   which we are looking at right now. As only one of cmin, cmax or combocid
 *   is actually stored in the heap we don't have access to the value we
 *   need anymore.
 *
 * To resolve those problems we have a per-transaction hash of (cmin,
 * cmax) tuples keyed by (relfilenode, ctid) which contains the actual
 * (cmin, cmax) values. That also takes care of combocids by simply
 * not caring about them at all. As we have the real cmin/cmax values
 * combocids aren't interesting.
 *
 * As we only care about catalog tuples here the overhead of this
 * hashtable should be acceptable.
 *
 * Heap rewrites complicate this a bit, check rewriteheap.c for
 * details.
 * -------------------------------------------------------------------------
 *
 * struct for qsort()ing mapping files by lsn somewhat efficiently
 */
typedef struct RewriteMappingFile {
    XLogRecPtr lsn;
    char fname[MAXPGPATH];
} RewriteMappingFile;

/*
 * Apply a single mapping file to tuplecid_data.
 *
 * The mapping file has to have been verified to be a) committed b) for our
 * transaction c) applied in LSN order.
 */
static void ApplyLogicalMappingFile(HTAB *tuplecid_data, Oid relid, const char *fname)
{
    char path[MAXPGPATH];
    int fd;
    int readBytes;
    LogicalRewriteMappingData map;
    int rc = 0;
    rc = sprintf_s(path, sizeof(path), "pg_llog/mappings/%s", fname);
    securec_check_ss(rc, "", "");
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
            errmsg("could not open file \"%s\": %m", path)));
    }

    while (true) {
        ReorderBufferTupleCidKey key;
        ReorderBufferTupleCidEnt *ent = NULL;
        ReorderBufferTupleCidEnt *new_ent = NULL;
        bool found = false;

        /* be careful about padding */
        rc = memset_s(&key, sizeof(ReorderBufferTupleCidKey), 0, sizeof(ReorderBufferTupleCidKey));
        securec_check(rc, "", "");

        /* read all mappings till the end of the file */
        readBytes = read(fd, &map, sizeof(LogicalRewriteMappingData));
        if (readBytes < 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("could not read file \"%s\": %m", path)));
        } else if (readBytes == 0) { /* EOF */
            break;
        } else if (readBytes != sizeof(LogicalRewriteMappingData)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("could not read file \"%s\", read %d instead of %d",
                       path, readBytes, (int32)sizeof(LogicalRewriteMappingData))));
        }

        key.relnode = map.old_node;
        ItemPointerCopy(&map.old_tid, &key.tid);

        ent = (ReorderBufferTupleCidEnt *)hash_search(tuplecid_data, (void *)&key, HASH_FIND, NULL);
        /* no existing mapping, no need to update */
        if (ent == NULL) {
            continue;
        }

        key.relnode = map.new_node;
        ItemPointerCopy(&map.new_tid, &key.tid);

        new_ent = (ReorderBufferTupleCidEnt *)hash_search(tuplecid_data, (void *)&key, HASH_ENTER, &found);

        if (found) {
            /*
             * Make sure the existing mapping makes sense. We sometime update
             * old records that did not yet have a cmax (e.g. pg_class' own
             * entry while rewriting it) during rewrites, so allow that.
             */
            Assert(ent->cmin == InvalidCommandId || ent->cmin == new_ent->cmin);
            Assert(ent->cmax == InvalidCommandId || ent->cmax == new_ent->cmax);
        } else {
            /* update mapping */
            new_ent->cmin = ent->cmin;
            new_ent->cmax = ent->cmax;
            new_ent->combocid = ent->combocid;
        }
    }
    (void)CloseTransientFile(fd);
}

/*
 * Check whether the TransactionOId 'xid' is in the pre-sorted array 'xip'.
 */
static bool TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
    return bsearch(&xid, xip, num, sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * qsort() comparator for sorting RewriteMappingFiles in LSN order.
 */
static int file_sort_by_lsn(const void *a_p, const void *b_p)
{
    RewriteMappingFile *a = *(RewriteMappingFile **)a_p;
    RewriteMappingFile *b = *(RewriteMappingFile **)b_p;

    if (XLByteLT(a->lsn, b->lsn))
        return -1;
    else if (XLByteLT(b->lsn, a->lsn))
        return 1;
    return 0;
}

/*
 * Apply any existing logical remapping files if there are any targeted at our
 * transaction for relid.
 */
static void UpdateLogicalMappings(HTAB *tuplecid_data, Oid relid, Snapshot snapshot)
{
    DIR *mapping_dir = NULL;
    struct dirent *mapping_de = NULL;
    List *files = NIL;
    ListCell *file = NULL;
    RewriteMappingFile **files_a = NULL;
    int off = 0;
    Oid dboid = IsSharedRelation(relid) ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    int rc = 0;
    mapping_dir = AllocateDir("pg_llog/mappings");
    while ((mapping_de = ReadDir(mapping_dir, "pg_llog/mappings")) != NULL) {
        Oid f_dboid;
        Oid f_relid;
        TransactionId f_mapped_xid;
        TransactionId f_create_xid;
        XLogRecPtr f_lsn;
        uint32 f_hi, f_lo;
        RewriteMappingFile *f = 0;

        if (strcmp(mapping_de->d_name, ".") == 0 || strcmp(mapping_de->d_name, "..") == 0) {
            continue;
        }

        /* Ignore files that aren't ours */
        if (strncmp(mapping_de->d_name, "map-", 4) != 0) {
            continue;
        }

        if (sscanf_s(mapping_de->d_name, LOGICAL_REWRITE_FORMAT, &f_dboid, &f_relid, &f_hi, &f_lo, &f_mapped_xid,
                     &f_create_xid) != 6) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("could not parse fname %s", mapping_de->d_name)));
        }

        f_lsn = (((uint64)f_hi) << 32) + f_lo;

        /* mapping for another database */
        if (f_dboid != dboid)
            continue;

        /* mapping for another relation */
        if (f_relid != relid)
            continue;

        /* did the creating transaction abort? */
        if (!TransactionIdDidCommit(f_create_xid))
            continue;

        /* not for our transaction */
        if (!TransactionIdInArray(f_mapped_xid, snapshot->subxip, snapshot->subxcnt))
            continue;

        /* ok, relevant, queue for apply */
        f = (RewriteMappingFile *)palloc(sizeof(RewriteMappingFile));
        f->lsn = f_lsn;
        rc = strcpy_s(f->fname, MAXPGPATH, mapping_de->d_name);
        securec_check(rc, "", "");
        files = lappend(files, f);
    }
    (void)FreeDir(mapping_dir);

    /* build array we can easily sort */
    files_a = (RewriteMappingFile **)palloc(list_length(files) * sizeof(RewriteMappingFile *));
    off = 0;
    foreach (file, files) {
        files_a[off++] = (RewriteMappingFile *)lfirst(file);
    }

    /* sort files so we apply them in LSN order */
    qsort(files_a, list_length(files), sizeof(RewriteMappingFile *), file_sort_by_lsn);

    for (off = 0; off < list_length(files); off++) {
        RewriteMappingFile *f = files_a[off];

        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("applying mapping: %s in %lu", f->fname, snapshot->subxip[0])));
        ApplyLogicalMappingFile(tuplecid_data, relid, f->fname);
        pfree(f);
        f = NULL;
    }
    list_free(files);
}

/*
 * Lookup cmin/cmax of a tuple, during logical decoding where we can't rely on
 * combocids.
 */
bool ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data, Snapshot snapshot, HeapTuple htup, Buffer buffer,
                                   CommandId *cmin, CommandId *cmax)
{
    ReorderBufferTupleCidKey key;
    ReorderBufferTupleCidEnt *ent = NULL;
    ForkNumber forkno = InvalidForkNumber;
    BlockNumber blockno = InvalidBlockNumber;
    bool updated_mapping = false;
    int rc = 0;
    /* be careful about padding */
    rc = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "", "");

    Assert(!BufferIsLocal(buffer));

    /*
     * get relfilenode from the buffer, no convenient way to access it other
     * than that.
     */
    BufferGetTag(buffer, &key.relnode, &forkno, &blockno);

    /* tuples can only be in the main fork */
    Assert(forkno == MAIN_FORKNUM);
    Assert(blockno == ItemPointerGetBlockNumber(&htup->t_self));

    ItemPointerCopy(&htup->t_self, &key.tid);

restart:
    ent = (ReorderBufferTupleCidEnt *)hash_search(tuplecid_data, (void *)&key, HASH_FIND, NULL);

    /*
     * failed to find a mapping, check whether the table was rewritten and
     * apply mapping if so, but only do that once - there can be no new
     * mappings while we are in here since we have to hold a lock on the
     * relation.
     */
    if (ent == NULL && !updated_mapping) {
        UpdateLogicalMappings(tuplecid_data, htup->t_tableOid, snapshot);
        /* now check but don't update for a mapping again */
        updated_mapping = true;
        goto restart;
    } else if (ent == NULL)
        return false;

    if (cmin != NULL)
        *cmin = ent->cmin;
    if (cmax != NULL)
        *cmax = ent->cmax;
    return true;
}
