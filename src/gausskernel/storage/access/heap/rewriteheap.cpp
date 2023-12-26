/* -------------------------------------------------------------------------
 *
 * rewriteheap.cpp
 *	  Support functions to rewrite tables.
 *
 * These functions provide a facility to completely rewrite a heap, while
 * preserving visibility information and update chains.
 *
 * INTERFACE
 *
 * The caller is responsible for creating the new heap, all catalog
 * changes, supplying the tuples to be written to the new heap, and
 * rebuilding indexes.	The caller must hold AccessExclusiveLock on the
 * target table, because we assume no one else is writing into it.
 *
 * To use the facility:
 *
 * begin_heap_rewrite
 * while (fetch next tuple)
 * {
 *	   if (tuple is dead)
 *		   rewrite_heap_dead_tuple
 *	   else
 *	   {
 *		   // do any transformations here if required
 *		   rewrite_heap_tuple
 *	   }
 * }
 * end_heap_rewrite
 *
 * The contents of the new relation shouldn't be relied on until after
 * end_heap_rewrite is called.
 *
 *
 * IMPLEMENTATION
 *
 * This would be a fairly trivial affair, except that we need to maintain
 * the ctid chains that link versions of an updated tuple together.
 * Since the newly stored tuples will have tids different from the original
 * ones, if we just copied t_ctid fields to the new table the links would
 * be wrong.  When we are required to copy a (presumably recently-dead or
 * delete-in-progress) tuple whose ctid doesn't point to itself, we have
 * to substitute the correct ctid instead.
 *
 * For each ctid reference from A -> B, we might encounter either A first
 * or B first.	(Note that a tuple in the middle of a chain is both A and B
 * of different pairs.)
 *
 * If we encounter A first, we'll store the tuple in the unresolved_tups
 * hash table. When we later encounter B, we remove A from the hash table,
 * fix the ctid to point to the new location of B, and insert both A and B
 * to the new heap.
 *
 * If we encounter B first, we can insert B to the new heap right away.
 * We then add an entry to the old_new_tid_map hash table showing B's
 * original tid (in the old heap) and new tid (in the new heap).
 * When we later encounter A, we get the new location of B from the table,
 * and can write A immediately with the correct ctid.
 *
 * Entries in the hash tables can be removed as soon as the later tuple
 * is encountered.	That helps to keep the memory usage down.  At the end,
 * both tables are usually empty; we should have encountered both A and B
 * of each pair.  However, it's possible for A to be RECENTLY_DEAD and B
 * entirely DEAD according to HeapTupleSatisfiesVacuum, because the test
 * for deadness using OldestXmin is not exact.	In such a case we might
 * encounter B first, and skip it, and find A later.  Then A would be added
 * to unresolved_tups, and stay there until end of the rewrite.  Since
 * this case is very unusual, we don't worry about the memory usage.
 *
 * Using in-memory hash tables means that we use some memory for each live
 * update chain in the table, from the time we find one end of the
 * reference until we find the other end.  That shouldn't be a problem in
 * practice, but if you do something like an UPDATE without a where-clause
 * on a large table, and then run CLUSTER in the same transaction, you
 * could run out of memory.  It doesn't seem worthwhile to add support for
 * spill-to-disk, as there shouldn't be that many RECENTLY_DEAD tuples in a
 * table under normal circumstances.  Furthermore, in the typical scenario
 * of CLUSTERing on an unchanging key column, we'll see all the versions
 * of a given tuple together anyway, and so the peak memory usage is only
 * proportional to the number of RECENTLY_DEAD versions of a single row, not
 * in the whole table.	Note that if we do fail halfway through a CLUSTER,
 * the old table is still valid, so failure is not catastrophic.
 *
 * We can't use the normal heap_insert function to insert into the new
 * heap, because heap_insert overwrites the visibility information.
 * We use a special-purpose raw_heap_insert function instead, which
 * is optimized for bulk inserting a lot of tuples, knowing that we have
 * exclusive access to the heap.  raw_heap_insert builds new pages in
 * local storage.  When a page is full, or at the end of the process,
 * we insert it to WAL as a single record and then write it to disk
 * directly through smgr.  Note, however, that any data sent to the new
 * heap's TOAST table will go through the normal bufmgr.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/heap/rewriteheap.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/catalog.h"

#include "lib/ilist.h"

#include "replication/logical.h"
#include "replication/slot.h"

#include "access/xloginsert.h"
#include "access/htup.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_utuptoaster.h"
#include "access/ustore/knl_uhio.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/fd.h"
#include "storage/pagecompress.h"
#include "storage/smgr/smgr.h"
#include "utils/aiomem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "commands/tablespace.h"
#include "utils/builtins.h"
#include "storage/procarray.h"

/* Max tuple number && size of one batch tuples */
const int DEFAULTBUFFEREDTUPLES = 10000;
const Size DEFAULTBUFFERSIZE = (4 * 1024 * 1024);

#define REWRITE_BUFFERS_QUEUE_COUNT 1024

/*
 * State associated with a rewrite operation. This is opaque to the user
 * of the rewrite facility.
 */
typedef struct RewriteStateData {
    Relation rs_old_rel;          /* source heap */
    Relation rs_new_rel;          /* destination heap */
    Page rs_buffer;               /* page currently being built */
    BlockNumber rs_blockno;       /* block where page will go */
    bool rs_buffer_valid;         /* T if any tuples in buffer */
    bool rs_use_wal;              /* must we WAL-log inserts? */
    TransactionId rs_oldest_xmin; /* oldest xmin used by caller to determine tuple visibility */
    TransactionId rs_freeze_xid;  /* Xid that will be used as freeze cutoff point */
    MultiXactId	rs_freeze_multi;  /* MultiXactId that will be used as freeze
                                   * cutoff point for multixacts */

    MemoryContext rs_cxt;        /* for hash tables and entries and tuples in them */
    HTAB *rs_unresolved_tups;    /* unmatched A tuples */
    HTAB *rs_old_new_tid_map;    /* unmatched B tuples */
    PageCompress *rs_compressor; /* use to compress tuples */
    Page rs_cmprBuffer;          /* sotre compressed tuples */
    HeapTuple *rs_tupBuf;        /* cache batch tuples for compressor */
    Size rs_size;                /* the size of this batch tuple */
    int rs_nTups;                /* the tuple number of this batch */
    bool rs_doCmprFlag;          /* whether do compress */

    char *rs_buffers_queue;             /* adio write queue */
    char *rs_buffers_queue_ptr;         /* adio write queue ptr */
    BufferDesc *rs_buffers_handler;     /* adio write buffer handler */
    BufferDesc *rs_buffers_handler_ptr; /* adio write buffer handler ptr */
    int rs_block_start;                 /* adio write start block id */
    int rs_block_count;                 /* adio write block count */
} RewriteStateData;

/*
 * The lookup keys for the hash tables are tuple TID and xmin (we must check
 * both to avoid false matches from dead tuples).  Beware that there is
 * probably some padding space in this struct; it must be zeroed out for
 * correct hashtable operation.
 */
typedef struct {
    TransactionId xmin;  /* tuple xmin */
    ItemPointerData tid; /* tuple location in old heap */
} TidHashKey;

/*
 * Entry structures for the hash tables
 */
typedef struct {
    TidHashKey key;          /* expected xmin/old location of B tuple */
    ItemPointerData old_tid; /* A's location in the old heap */
    HeapTuple tuple;         /* A's tuple contents */
} UnresolvedTupData;

typedef UnresolvedTupData *UnresolvedTup;

typedef struct {
    TidHashKey key;          /* actual xmin/old location of B tuple */
    ItemPointerData new_tid; /* where we put it in the new heap */
} OldToNewMappingData;

typedef OldToNewMappingData *OldToNewMapping;

/* prototypes for internal functions */
static void raw_heap_insert(RewriteState state, HeapTuple tup);
static void RawUHeapInsert(RewriteState state, UHeapTuple tup);
static void RawHeapCmprAndMultiInsert(RewriteState state, bool is_last);
static void copyHeapTupleInfo(HeapTuple dest_tup, HeapTuple src_tup, TransactionId freeze_xid, MultiXactId freeze_mxid);
#ifndef ENABLE_LITE_MODE
static void rewrite_page_list_write(RewriteState state);
#endif
static void rewrite_flush_page(RewriteState state, Page page);
static void rewrite_end_flush_page(RewriteState state);
static void rewrite_write_one_page(RewriteState state, Page page);

/*
 * Begin a rewrite of a table
 *
 * old_heap        old, locked heap relation tuples will be read from
 * new_heap		new, locked heap relation to insert tuples to
 * oldest_xmin	xid used by the caller to determine which tuples are dead
 * freeze_xid	xid before which tuples will be frozen
 * use_wal		should the inserts to the new heap be WAL-logged?
 *
 * Returns an opaque RewriteState, allocated in current memory context,
 * to be used in subsequent calls to the other functions.
 */
RewriteState begin_heap_rewrite(Relation old_heap, Relation new_heap, TransactionId oldest_xmin,
                                TransactionId freeze_xid, bool use_wal)
{
    RewriteState state;
    MemoryContext rw_cxt;
    MemoryContext old_cxt;
    HASHCTL hash_ctl;
    errno_t errorno = EOK;
    char* unalign_cmprBuffer = NULL;
    char* unalign_rsBuffer = NULL;

    /*
     * To ease cleanup, make a separate context that will contain the
     * RewriteState struct itself plus all subsidiary data.
     */
    rw_cxt = AllocSetContextCreate(CurrentMemoryContext, "Table rewrite", ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    old_cxt = MemoryContextSwitchTo(rw_cxt);

    /* Create and fill in the state struct */
    state = (RewriteStateData *)palloc0(sizeof(RewriteStateData));
    state->rs_old_rel = old_heap;

    state->rs_new_rel = new_heap;
    /* new_heap needn't be empty, just locked */
    state->rs_blockno = RelationGetNumberOfBlocks(new_heap);
    ADIO_RUN()
    {
        state->rs_buffer = (Page)adio_align_alloc(BLCKSZ);
        state->rs_buffers_queue_ptr = (char *)adio_align_alloc(BLCKSZ * REWRITE_BUFFERS_QUEUE_COUNT * 2);
        state->rs_buffers_queue = state->rs_buffers_queue_ptr;
        state->rs_buffers_handler_ptr = (BufferDesc *)palloc(sizeof(BufferDesc) * REWRITE_BUFFERS_QUEUE_COUNT * 2);
        state->rs_buffers_handler = state->rs_buffers_handler_ptr;
        state->rs_block_start = state->rs_blockno;
        state->rs_block_count = 0;

        for (int i = 0; i < REWRITE_BUFFERS_QUEUE_COUNT * 2; i++) {
            pg_atomic_init_u64(&(state->rs_buffers_handler[i].state), 0);
        }
    }
    ADIO_ELSE()
    {
        if (ENABLE_DSS) {
            unalign_rsBuffer = (char*)palloc(BLCKSZ + ALIGNOF_BUFFER);
            state->rs_buffer = (Page)BUFFERALIGN(unalign_rsBuffer);
        } else {
            state->rs_buffer = (Page)palloc(BLCKSZ);
        }
    }
    ADIO_END();

    state->rs_buffer_valid = false;
    state->rs_use_wal = use_wal;
    state->rs_oldest_xmin = oldest_xmin;
    state->rs_freeze_xid = freeze_xid;
    state->rs_cxt = rw_cxt;

    /*
     * even new_heap is a partitional relation, its rd_rel is copied from its pareent
     * relation. so don't worry the compress property about new_heap;
     */
    if (!RelationIsUstoreFormat(old_heap))
        state->rs_doCmprFlag = RowRelationIsCompressed(new_heap);

    if (state->rs_doCmprFlag) {
        state->rs_compressor = New(rw_cxt) PageCompress(new_heap, rw_cxt);
        ADIO_RUN()
        {
            state->rs_cmprBuffer = (Page)adio_align_alloc(BLCKSZ);
            errorno = memset_s(state->rs_cmprBuffer, BLCKSZ, 0, BLCKSZ);
            securec_check(errorno, "", "");
        }
        ADIO_ELSE()
        {
            if (ENABLE_DSS) {
                unalign_cmprBuffer = (char*)palloc0(BLCKSZ + ALIGNOF_BUFFER);
                state->rs_cmprBuffer = (Page)BUFFERALIGN(unalign_cmprBuffer);
            } else {
                state->rs_cmprBuffer = (Page)palloc0(BLCKSZ);
            }
        }
        ADIO_END();
        state->rs_tupBuf = (HeapTuple *)palloc(sizeof(HeapTuple) * DEFAULTBUFFEREDTUPLES);
        state->rs_nTups = 0;
        state->rs_size = 0;
    }

    /* Initialize hash tables used to track update chains */
    errorno = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(errorno, "", "");
    hash_ctl.keysize = sizeof(TidHashKey);
    hash_ctl.entrysize = sizeof(UnresolvedTupData);
    hash_ctl.hcxt = state->rs_cxt;
    hash_ctl.hash = tag_hash;

    state->rs_unresolved_tups = hash_create("Rewrite / Unresolved ctids", 128, /* arbitrary initial size */
                                            &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    hash_ctl.entrysize = sizeof(OldToNewMappingData);

    state->rs_old_new_tid_map = hash_create("Rewrite / Old to new tid map", 128, /* arbitrary initial size */
                                            &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    (void)MemoryContextSwitchTo(old_cxt);

    return state;
}

static void rewrite_write_one_page(RewriteState state, Page page)
{
    TdeInfo tde_info = {0};
    if (RelationisEncryptEnable(state->rs_new_rel)) {
        GetTdeInfoFromRel(state->rs_new_rel, &tde_info);
    }
    if (IsSegmentFileNode(state->rs_new_rel->rd_node)) {
        Assert(state->rs_use_wal);
        Buffer buf = ReadBuffer(state->rs_new_rel, P_NEW);
#ifdef USE_ASSERT_CHECKING
        BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);
        Assert(buf_desc->tag.blockNum == state->rs_blockno);
#endif
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        XLogRecPtr xlog_ptr = log_newpage(&state->rs_new_rel->rd_node, MAIN_FORKNUM, state->rs_blockno, page, true, 
                                          &tde_info);
        errno_t rc = memcpy_s(BufferGetBlock(buf), BLCKSZ, page, BLCKSZ);
        securec_check(rc, "\0", "\0");
        PageSetLSN(BufferGetPage(buf), xlog_ptr);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }  else {
        /* check tablespace size limitation when extending new file. */
        STORAGE_SPACE_OPERATION(state->rs_new_rel, BLCKSZ);

        if (state->rs_use_wal) {
            log_newpage(&state->rs_new_rel->rd_node, MAIN_FORKNUM, state->rs_blockno, page, true, &tde_info);
        }

        RelationOpenSmgr(state->rs_new_rel);

        char *bufToWrite = NULL;
        if (RelationisEncryptEnable(state->rs_new_rel)) {
            bufToWrite = PageDataEncryptIfNeed(page, &tde_info, true);
        } else {
            bufToWrite = page;
        }

        PageSetChecksumInplace((Page)bufToWrite, state->rs_blockno);

        rewrite_flush_page(state, (Page)bufToWrite);
    }
}

/*
 * End a rewrite.
 *
 * state and any other resources are freed.
 */
void end_heap_rewrite(RewriteState state)
{
    HASH_SEQ_STATUS seq_status;
    UnresolvedTup unresolved = NULL;

    if (state->rs_doCmprFlag) {
        if (state->rs_nTups > 0)
            RawHeapCmprAndMultiInsert(state, true);

        /*
         * log and write the last compressed page.
         * at the last time, it's the worst result that the last two pages are not full of tuples.
         * but it's ok because only either one or two page will be used partly.
         */
        Page page = state->rs_cmprBuffer;
        if (!PageIsEmpty(page)) {
            rewrite_write_one_page(state, page);
            state->rs_blockno++;
        }
        delete state->rs_compressor;
    }

    /*
     * Write any remaining tuples in the UnresolvedTups table. If we have any
     * left, they should in fact be dead, but let's err on the safe side.
     */
    hash_seq_init(&seq_status, state->rs_unresolved_tups);

    while ((unresolved = (UnresolvedTupData *)hash_seq_search(&seq_status)) != NULL) {
        ItemPointerSetInvalid(&unresolved->tuple->t_data->t_ctid);
        raw_heap_insert(state, unresolved->tuple);
    }

    /* Write the last page, if any */
    if (state->rs_buffer_valid) {
        rewrite_write_one_page(state, state->rs_buffer);
    }

    rewrite_end_flush_page(state);

    /*
     * If the rel is WAL-logged, must fsync before commit.	We use heap_sync
     * to ensure that the toast table gets fsync'd too.
     *
     * It's obvious that we must do this when not WAL-logging. It's less
     * obvious that we have to do it even if we did WAL-log the pages. The
     * reason is the same as in tablecmds.c's copy_relation_data(): we're
     * writing data that's not in shared buffers, and so a CHECKPOINT
     * occurring during the rewriteheap operation won't have fsync'd data we
     * wrote before the checkpoint.
     */
    if (RelationNeedsWAL(state->rs_new_rel) && !RelationIsBucket(state->rs_new_rel))
        heap_sync(state->rs_new_rel);

    ADIO_RUN()
    {
        if (state->rs_cmprBuffer != NULL) {
            adio_align_free(state->rs_cmprBuffer);
        }
        adio_align_free(state->rs_buffer);
        adio_align_free(state->rs_buffers_queue_ptr);
        pfree(state->rs_buffers_handler_ptr);
    }
    ADIO_END();

    /* Deleting the context frees everything */
    MemoryContextDelete(state->rs_cxt);
}

template <bool needCopy>
static bool CanWriteUpdatedTuple(RewriteState state, HeapTuple old_tuple, HeapTuple new_tuple)
{
    TidHashKey hashkey;
    bool found = false;

    errno_t rc = EOK;
    rc = memset_s(&hashkey, sizeof(hashkey), 0, sizeof(hashkey));
    securec_check(rc, "", "");

    /*
     * If the tuple has been updated, check the old-to-new mapping hash table.
     */
    if (!((old_tuple->t_data->t_infomask & HEAP_XMAX_INVALID) || HeapTupleIsOnlyLocked(old_tuple)) &&
        !(ItemPointerEquals(&(old_tuple->t_self), &(old_tuple->t_data->t_ctid)))) {
        OldToNewMapping mapping = NULL;

        hashkey.xmin = HeapTupleGetUpdateXid(old_tuple);
        hashkey.tid = old_tuple->t_data->t_ctid;

        mapping = (OldToNewMapping)hash_search(state->rs_old_new_tid_map, &hashkey, HASH_FIND, NULL);
        if (mapping != NULL) {
            /*
             * We've already copied the tuple that t_ctid points to, so we can
             * set the ctid of this tuple to point to the new location, and
             * insert it right away.
             */
            new_tuple->t_data->t_ctid = mapping->new_tid;

            /* We don't need the mapping entry anymore */
            (void)hash_search(state->rs_old_new_tid_map, &hashkey, HASH_REMOVE, &found);
            Assert(found);
        } else {
            /*
             * We haven't seen the tuple t_ctid points to yet. Stash this
             * tuple into unresolved_tups to be written later.
             */
            UnresolvedTup unresolved;

            unresolved = (UnresolvedTup)hash_search(state->rs_unresolved_tups, &hashkey, HASH_ENTER, &found);
            Assert(!found);

            unresolved->old_tid = old_tuple->t_self;
            unresolved->tuple = needCopy ? heap_copytuple(new_tuple) : new_tuple;

            return false;
        }
    }

    return true;
}

/*
 * Add a tuple to the new heap.
 *
 * Visibility information is copied from the original tuple, except that
 * we "freeze" very-old tuples.  Note that since we scribble on new_tuple,
 * it had better be temp storage not a pointer to the original tuple.
 *
 * state		opaque state as returned by begin_heap_rewrite
 * old_tuple	original tuple in the old heap
 * new_tuple	new, rewritten tuple to be inserted to new heap
 */
void rewrite_heap_tuple(RewriteState state, HeapTuple old_tuple, HeapTuple new_tuple)
{
    Assert(TUPLE_IS_HEAP_TUPLE(old_tuple));
    Assert(TUPLE_IS_HEAP_TUPLE(new_tuple));

    MemoryContext old_cxt;
    ItemPointerData old_tid;
    TidHashKey hashkey;
    bool found = false;
    bool free_new = false;

    old_cxt = MemoryContextSwitchTo(state->rs_cxt);

    copyHeapTupleInfo(new_tuple, old_tuple, state->rs_freeze_xid, state->rs_freeze_multi);

    if (!CanWriteUpdatedTuple<true>(state, old_tuple, new_tuple)) {
        /*
         * We can't do anything more now, since we don't know where the
         * tuple will be written.
         */
        (void)MemoryContextSwitchTo(old_cxt);

        return;
    }

    /*
     * Now we will write the tuple, and then check to see if it is the B tuple
     * in any new or known pair.  When we resolve a known pair, we will be
     * able to write that pair's A tuple, and then we have to check if it
     * resolves some other pair.  Hence, we need a loop here.
     */
    old_tid = old_tuple->t_self;
    free_new = false;

    for (;;) {
        ItemPointerData new_tid;

        /* Insert the tuple and find out where it's put in new_heap */
        raw_heap_insert(state, new_tuple);
        new_tid = new_tuple->t_self;

        /*
         * If the tuple is the updated version of a row, and the prior version
         * wouldn't be DEAD yet, then we need to either resolve the prior
         * version (if it's waiting in rs_unresolved_tups), or make an entry
         * in rs_old_new_tid_map (so we can resolve it when we do see it). The
         * previous tuple's xmax would equal this one's xmin, so it's
         * RECENTLY_DEAD if and only if the xmin is not before OldestXmin.
         */
        if ((new_tuple->t_data->t_infomask & HEAP_UPDATED) &&
            !TransactionIdPrecedes(HeapTupleGetRawXmin(new_tuple), state->rs_oldest_xmin)) {
            /*
             * Okay, this is B in an update pair.  See if we've seen A.
             */
            UnresolvedTup unresolved = NULL;

            errno_t rc = memset_s(&hashkey, sizeof(hashkey), 0, sizeof(hashkey));
            securec_check(rc, "", "");

            hashkey.xmin = HeapTupleGetRawXmin(new_tuple);
            hashkey.tid = old_tid;

            unresolved = (UnresolvedTup)hash_search(state->rs_unresolved_tups, &hashkey, HASH_FIND, NULL);
            if (unresolved != NULL) {
                /*
                 * We have seen and memorized the previous tuple already. Now
                 * that we know where we inserted the tuple its t_ctid points
                 * to, fix its t_ctid and insert it to the new heap.
                 */
                if (free_new)
                    heap_freetuple(new_tuple);
                new_tuple = unresolved->tuple;
                free_new = true;
                old_tid = unresolved->old_tid;
                new_tuple->t_data->t_ctid = new_tid;

                /*
                 * We don't need the hash entry anymore, but don't free its
                 * tuple just yet.
                 */
                (void)hash_search(state->rs_unresolved_tups, &hashkey, HASH_REMOVE, &found);
                Assert(found);

                /* loop back to insert the previous tuple in the chain */
                continue;
            } else {
                /*
                 * Remember the new tid of this tuple. We'll use it to set the
                 * ctid when we find the previous tuple in the chain.
                 */
                OldToNewMapping mapping;

                mapping = (OldToNewMapping)hash_search(state->rs_old_new_tid_map, &hashkey, HASH_ENTER, &found);
                Assert(!found);

                mapping->new_tid = new_tid;
            }
        }

        /* Done with this (chain of) tuples, for now */
        if (free_new)
            heap_freetuple(new_tuple);
        break;
    }

    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * Add a uheap tuple to the new heap.
 *
 * Maintaining previous version's visibility information needs much more work,
 * so for now, we freeze all the tuples.  We only get
 * LIVE versions of the tuple as input.
 *
 * state                opaque state as returned by begin_heap_rewrite
 * oldTuple    original tuple in the old heap
 * newTuple    new, rewritten tuple to be inserted to new heap
 */
void
RewriteUHeapTuple(RewriteState state,
                                   UHeapTuple oldTuple, UHeapTuple newTuple)
{
    Assert(oldTuple->tupTableType == UHEAP_TUPLE);
    Assert(newTuple->tupTableType == UHEAP_TUPLE);
    MemoryContext old_cxt;

    old_cxt = MemoryContextSwitchTo(state->rs_cxt);

    /*
     * As of now, we copy only LIVE tuples in UHeap, so we can mark them as
     * frozen.
     */
    newTuple->disk_tuple->flag &= ~UHEAP_VIS_STATUS_MASK;
    newTuple->disk_tuple->xid = (ShortTransactionId)FrozenTransactionId;
    UHeapTupleHeaderSetTDSlot(newTuple->disk_tuple, UHEAPTUP_SLOT_FROZEN);

    /* Insert the tuple and find out where it's put in new_heap */
    RawUHeapInsert(state, newTuple);

    MemoryContextSwitchTo(old_cxt);
    FastVerifyUTuple(newTuple->disk_tuple, InvalidBuffer);
}


bool use_heap_rewrite_memcxt(RewriteState state)
{
    return state->rs_doCmprFlag;
}

MemoryContext get_heap_rewrite_memcxt(RewriteState state)
{
    return state->rs_cxt;
}

static void copyHeapTupleInfo(HeapTuple dest_tup, HeapTuple src_tup, TransactionId freeze_xid, MultiXactId freeze_mxid)
{
    /*
     * Copy the original tuple's visibility information into new_tuple.
     *
     * XXX we might later need to copy some t_infomask2 bits, too? Right now,
     * we intentionally clear the HOT status bits.
     */
    errno_t rc = EOK;

    rc = memcpy_s(&dest_tup->t_data->t_choice.t_heap, sizeof(HeapTupleFields), &src_tup->t_data->t_choice.t_heap,
                  sizeof(HeapTupleFields));
    securec_check(rc, "", "");

    HeapTupleCopyBase(dest_tup, src_tup);
    dest_tup->t_data->t_infomask &= ~HEAP_XACT_MASK;
    dest_tup->t_data->t_infomask2 &= ~HEAP2_XACT_MASK;
    dest_tup->t_data->t_infomask |= src_tup->t_data->t_infomask & HEAP_XACT_MASK;

    /*
     * While we have our hands on the tuple, we may as well freeze any
     * very-old xmin or xmax, so that future VACUUM effort can be saved.
     */
    (void)heap_freeze_tuple(dest_tup, freeze_xid, freeze_mxid);

    /*
     * Invalid ctid means that ctid should point to the tuple itself. We'll
     * override it later if the tuple is part of an update chain.
     */
    ItemPointerSetInvalid(&dest_tup->t_data->t_ctid);
}

static void prepare_cmpr_buffer(RewriteState state, Size meta_size, const char *meta_data)
{
    Page page = state->rs_cmprBuffer;
    errno_t rc = EOK;
    HeapPageHeader phdr;

    /*
     * at the first time, page is allocated by palloc0(), so that IF condition is true;
     * when page is full of tuples after compression, this IF condition is false. so that
     * total page will be logged and then written into heap disk.
     */
    if (!PageIsEmpty(page)) {
        if (IsSegmentFileNode(state->rs_new_rel->rd_node)) {
            Assert(state->rs_use_wal);

            Buffer buf = ReadBuffer(state->rs_new_rel, P_NEW);
#ifdef USE_ASSERT_CHECKING
            BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);
            Assert(buf_desc->tag.blockNum == state->rs_blockno);
#endif
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            XLogRecPtr xlog_ptr = log_newpage(&state->rs_new_rel->rd_node, MAIN_FORKNUM, state->rs_blockno, page, true);

            errno_t rc = memcpy_s(BufferGetBlock(buf), BLCKSZ, page, BLCKSZ);
            securec_check(rc, "\0", "\0");
            PageSetLSN(BufferGetPage(buf), xlog_ptr);
            MarkBufferDirty(buf);

            UnlockReleaseBuffer(buf);
        } 
        rewrite_write_one_page(state, page);
        state->rs_blockno++;
    }

    /* init compressed page */
    Assert(meta_size > 0 && meta_size < BLCKSZ);
    PageInit(page, BLCKSZ, 0, true);
    phdr = (HeapPageHeader)page;
    phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    phdr->pd_multi_base = 0;

    PageReinitWithDict(page, meta_size);
    Assert(PageIsCompressed(page) && (meta_data != NULL));

    rc = memcpy_s((char *)getPageDict(page), meta_size, meta_data, meta_size);
    securec_check(rc, "", "");
}

typedef void (*insert_tuple_func)(RewriteState state, HeapTuple tuple);

/* Insert a tuple to the new relation after compression. */
static void cmpr_heap_insert(RewriteState state, HeapTuple tup)
{
    Page page = state->rs_cmprBuffer;
    Size len;
    OffsetNumber newoff;
    TransactionId xmin, xmax;

    Assert(state->rs_new_rel->rd_rel->relkind != RELKIND_TOASTVALUE);
    Assert(!HeapTupleHasExternal(tup) && !(tup->t_len > TOAST_TUPLE_THRESHOLD));

    xmin = HeapTupleGetRawXmin(tup);
    xmax = HeapTupleGetRawXmax(tup);
    rewrite_page_prepare_for_xid(page, xmin, false);
    (void)rewrite_page_prepare_for_xid(page, xmax, (tup->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ? true : false);

    HeapTupleCopyBaseFromPage(tup, page);
    HeapTupleSetXmin(tup, xmin);
    HeapTupleSetXmax(tup, xmax);

    len = MAXALIGN(tup->t_len);
    if (len > MaxHeapTupleSize)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("row is too big: size %lu, maximum size %lu",
                                                                 (unsigned long)len, (unsigned long)MaxHeapTupleSize)));

    Assert(PageIsCompressed(page));

    /* And now we can insert the tuple into the page */
    newoff = PageAddItem(page, (Item)tup->t_data, tup->t_len, InvalidOffsetNumber, false, true);
    Assert(newoff != InvalidOffsetNumber);

    /* Update caller's t_self to the actual position where it was stored */
    ItemPointerSet(&(tup->t_self), state->rs_blockno, newoff);

    /* Now Insert the correct position into CTID of the stored tuple too. */
    if (!ItemPointerIsValid(&tup->t_data->t_ctid)) {
        ItemId newitemid = PageGetItemId(page, newoff);
        HeapTupleHeader onpage_tup = (HeapTupleHeader)PageGetItem(page, newitemid);
        onpage_tup->t_ctid = tup->t_self;
    }
}

static void RawHeapCmprAndMultiInsert(RewriteState state, bool is_last)
{
    PageCompress *compressor = state->rs_compressor;
    compressor->SetBatchTuples(state->rs_tupBuf, state->rs_nTups, is_last);

    while (compressor->CompressOnePage()) {
        insert_tuple_func insert_tuple = NULL;
        Size meta_size = (Size)compressor->GetCmprHeaderSize();
        HeapTuple *tuples = compressor->GetOutputTups();
        int ntuples = compressor->GetOutputCount();

        Assert(ntuples > 0);
        if (meta_size > 0) {
            insert_tuple = cmpr_heap_insert;
            compressor->ForwardWrite();
            prepare_cmpr_buffer(state, meta_size, compressor->GetCmprHeaderData());
        } else {
            /*
             * the case, when no compressed tuple is output, don't make sure that
             * those tuples fill the right one page perfectly. so call raw_heap_insert.
             */
            insert_tuple = raw_heap_insert;
        }

        for (int i = 0; i < ntuples; ++i) {
            insert_tuple(state, tuples[i]);
        }
    }

    if (is_last) {
        return;
    }

    /* after the end of WHILE loop, we should handle the reaminings */
    int remain = compressor->Remains();
    int total = state->rs_nTups - remain;
    Size remainSize = state->rs_size;
    HeapTuple *buffer = state->rs_tupBuf;

    for (int i = 0; i < total; ++i) {
        pfree(buffer[i]);
    }

    if (remain != state->rs_nTups) {
        remainSize = 0;
        for (int i = 0; i < remain; ++i) {
            buffer[i] = buffer[total + i];
            remainSize += buffer[i]->t_len;
        }
    }

    Assert((remain == 0 && remainSize == 0) || (remain == state->rs_nTups && remainSize == state->rs_size) ||
           (remain > 0 && remainSize < state->rs_size));
    state->rs_nTups = remain;
    state->rs_size = remainSize;
}

/* the same to rewrite_heap_tuple mostly, but compression before inserting */
void RewriteAndCompressTup(RewriteState state, HeapTuple old_tuple, HeapTuple new_tuple)
{
    ItemPointerData old_tid;
    TidHashKey hashkey;
    bool found = false;
    errno_t rc = EOK;

    Assert(CurrentMemoryContext == state->rs_cxt);
    copyHeapTupleInfo(new_tuple, old_tuple, state->rs_freeze_xid, state->rs_freeze_multi);

    /*
     * Step 1: deal with updated tuples chain.
     * all the tuples within updated chains are neither buffered nor compressed;
     */
    if (!CanWriteUpdatedTuple<false>(state, old_tuple, new_tuple)) {
        return;
    }

    old_tid = old_tuple->t_self;
    for (;;) {
        if ((new_tuple->t_data->t_infomask & HEAP_UPDATED) &&
            !TransactionIdPrecedes(HeapTupleGetRawXmin(new_tuple), state->rs_oldest_xmin)) {
            /*
             * Okay, this is B in an update pair.  See if we've seen A.
             */
            UnresolvedTup unresolved = NULL;
            ItemPointerData new_tid;

            /* Insert the tuple and find out where it's put in new_heap */
            raw_heap_insert(state, new_tuple);
            new_tid = new_tuple->t_self;

            rc = memset_s(&hashkey, sizeof(hashkey), 0, sizeof(hashkey));
            securec_check(rc, "", "");
            hashkey.xmin = HeapTupleGetRawXmin(new_tuple);
            hashkey.tid = old_tid;

            /*
             * until now new_tuple has been inserted into page, and it's not used later;
             * so we can free it under both IF and ELSE conditions;
             */
            heap_freetuple(new_tuple);

            unresolved = (UnresolvedTup)hash_search(state->rs_unresolved_tups, &hashkey, HASH_FIND, NULL);
            if (unresolved != NULL) {
                /*
                 * We have seen and memorized the previous tuple already. Now
                 * that we know where we inserted the tuple its t_ctid points
                 * to, fix its t_ctid and insert it to the new heap.
                 */
                new_tuple = unresolved->tuple;
                old_tid = unresolved->old_tid;
                new_tuple->t_data->t_ctid = new_tid;

                /*
                 * We don't need the hash entry anymore, but don't free its
                 * tuple just yet.
                 */
                (void)hash_search(state->rs_unresolved_tups, &hashkey, HASH_REMOVE, &found);
                Assert(found);

                /* loop back to insert the previous tuple in the chain */
                continue;
            } else {
                /*
                 * Remember the new tid of this tuple. We'll use it to set the
                 * ctid when we find the previous tuple in the chain.
                 */
                OldToNewMapping mapping;

                mapping = (OldToNewMapping)hash_search(state->rs_old_new_tid_map, &hashkey, HASH_ENTER, &found);
                Assert(!found);

                mapping->new_tid = new_tid;
                return;
            }
        }

        break;
    }

    /*
     * Step 2: Put this tuple into state->rs_tupBuf
     * We need hold memory for new_tuple
     */
    state->rs_tupBuf[state->rs_nTups++] = new_tuple;
    state->rs_size += new_tuple->t_len;

    /* Step 3: Compress this batch if it is full */
    if (state->rs_nTups >= DEFAULTBUFFEREDTUPLES || state->rs_size >= DEFAULTBUFFERSIZE)
        RawHeapCmprAndMultiInsert(state, false);
}

/*
 * Register a dead tuple with an ongoing rewrite. Dead tuples are not
 * copied to the new table, but we still make note of them so that we
 * can release some resources earlier.
 *
 * Returns true if a tuple was removed from the unresolved_tups table.
 * This indicates that that tuple, previously thought to be "recently dead",
 * is now known really dead and won't be written to the output.
 */
bool rewrite_heap_dead_tuple(RewriteState state, HeapTuple old_tuple)
{
    /*
     * If we have already seen an earlier tuple in the update chain that
     * points to this tuple, let's forget about that earlier tuple. It's in
     * fact dead as well, our simple xmax < OldestXmin test in
     * HeapTupleSatisfiesVacuum just wasn't enough to detect it. It happens
     * when xmin of a tuple is greater than xmax, which sounds
     * counter-intuitive but is perfectly valid.
     *
     * We don't bother to try to detect the situation the other way round,
     * when we encounter the dead tuple first and then the recently dead one
     * that points to it. If that happens, we'll have some unmatched entries
     * in the UnresolvedTups hash table at the end. That can happen anyway,
     * because a vacuum might have removed the dead tuple in the chain before
     * us.
     */
    UnresolvedTup unresolved = NULL;
    TidHashKey hashkey;
    bool found = false;
    errno_t rc = EOK;

    rc = memset_s(&hashkey, sizeof(hashkey), 0, sizeof(hashkey));
    securec_check(rc, "", "");
    hashkey.xmin = HeapTupleGetRawXmin(old_tuple);
    hashkey.tid = old_tuple->t_self;

    unresolved = (UnresolvedTup)hash_search(state->rs_unresolved_tups, &hashkey, HASH_FIND, NULL);
    if (unresolved != NULL) {
        /* Need to free the contained tuple as well as the hashtable entry */
        heap_freetuple(unresolved->tuple);
        (void)hash_search(state->rs_unresolved_tups, &hashkey, HASH_REMOVE, &found);
        Assert(found);
        return true;
    }

    return false;
}

#ifndef ENABLE_LITE_MODE
/*
 * @Description: vacuum full use this api to list write block by adio.   aioDescp->blockDesc.bufHdr = NULL; to figure
 * this is vacuum operate
 * @Param[IN] state: Rewrite State
 * @See also:
 */
void rewrite_page_list_write(RewriteState state)
{
    AioDispatchDesc_t **d_list;
    SMgrRelation smgr_reln = state->rs_new_rel->rd_smgr;
    char *buf_list = state->rs_buffers_queue;
    int32 start = state->rs_block_start;
    int32 n_bufs = state->rs_block_count;

    t_thrd.storage_cxt.InProgressAioBuf = NULL;
    t_thrd.storage_cxt.InProgressAioDispatch =
        (AioDispatchDesc_t **)palloc(sizeof(AioDispatchDesc_t *) * MAX_BACKWRITE_REQSIZ);
    d_list = t_thrd.storage_cxt.InProgressAioDispatch;

    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioVacummFull;

    for (int i = 0; i < n_bufs; i++) {
        AioDispatchDesc_t *aioDescp = NULL;
        BufferDesc *bufHdr = (BufferDesc *)(state->rs_buffers_handler + i);
        uint64 buf_state;

        /*
         * Allocate an iocb, fill it in, and write the addr in the
         * dList array.
         */
        aioDescp = (AioDispatchDesc_t *)adio_share_alloc(sizeof(AioDispatchDesc_t));

        buf_state = LockBufHdr(bufHdr);
        buf_state |= BM_IO_IN_PROGRESS;
        UnlockBufHdr(bufHdr, buf_state);

        /* iocb filled in later */
        aioDescp->aiocb.data = 0;
        aioDescp->aiocb.aio_fildes = 0;
        aioDescp->aiocb.aio_lio_opcode = 0;
        aioDescp->aiocb.u.c.buf = 0;
        aioDescp->aiocb.u.c.nbytes = 0;
        aioDescp->aiocb.u.c.offset = 0;

        /* AIO block descriptor filled here */
        aioDescp->blockDesc.smgrReln = smgr_reln;
        aioDescp->blockDesc.forkNum = MAIN_FORKNUM;
        aioDescp->blockDesc.blockNum = start + i;
        aioDescp->blockDesc.buffer = (char *)(buf_list + i * BLCKSZ);
        aioDescp->blockDesc.blockSize = BLCKSZ;
        aioDescp->blockDesc.reqType = PageListBackWriteType;
        aioDescp->blockDesc.bufHdr = bufHdr;
        aioDescp->blockDesc.descType = AioVacummFull;

        d_list[t_thrd.storage_cxt.InProgressAioDispatchCount++] = aioDescp;

        /*
         * Submit the I/O if the dispatch list is full and refill the dlist.
         */
        if (t_thrd.storage_cxt.InProgressAioDispatchCount >= MAX_BACKWRITE_REQSIZ) {
            HOLD_INTERRUPTS();
            /*
             * just get the info from the first one
             */
            smgrasyncwrite(d_list[0]->blockDesc.smgrReln, d_list[0]->blockDesc.forkNum, d_list,
                           t_thrd.storage_cxt.InProgressAioDispatchCount);
            t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
            RESUME_INTERRUPTS();
        }
    } /* for each buf in nBufs */

    if (t_thrd.storage_cxt.InProgressAioDispatchCount > 0) {
        HOLD_INTERRUPTS();
        smgrasyncwrite(d_list[0]->blockDesc.smgrReln, d_list[0]->blockDesc.forkNum, d_list,
                       t_thrd.storage_cxt.InProgressAioDispatchCount);
        t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
        RESUME_INTERRUPTS();
    }

    pfree(d_list);
    t_thrd.storage_cxt.InProgressAioDispatch = NULL;
    t_thrd.storage_cxt.InProgressAioDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;

    return;
}
#endif

/*
 * @Description: rewrite flush page
 * @Param[IN] page: page
 * @Param[IN] state:  RewriteState
 * @See also:
 *
 * Notice: caller must set the page checksum
 */
static void rewrite_flush_page(RewriteState state, Page page)
{
#ifndef ENABLE_LITE_MODE
    /* check aio is ready, for init db in single mode, no aio thread */
    if (AioCompltrIsReady() && g_instance.attr.attr_storage.enable_adio_function) {
        /* pass null buffer to lower levels to use fallocate, systables do not use fallocate,
         * relation id can distinguish systable or use table. "FirstNormalObjectId".
         * but unfortunately , but in standby, there is no relation id, so relation id has no work.
         * relation file node can not help becasue operation vacuum full or set table space can
         * change systable file node
         */
        if (u_sess->attr.attr_sql.enable_fast_allocate) {
            smgrextend(state->rs_new_rel->rd_smgr, MAIN_FORKNUM, state->rs_blockno, NULL, true);
        } else {
            smgrextend(state->rs_new_rel->rd_smgr, MAIN_FORKNUM, state->rs_blockno, (char *)page, true);
        }

        errno_t rc = memcpy_s((state->rs_buffers_queue + state->rs_block_count * BLCKSZ), BLCKSZ, (char *)page, BLCKSZ);
        securec_check(rc, "", "");
        state->rs_block_count++;

        if (state->rs_block_count >= REWRITE_BUFFERS_QUEUE_COUNT) {
            /* list write */
            rewrite_page_list_write(state);
            ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("rewrite_page_list_write, start(%d) count(%d)",
                                                         state->rs_block_start, state->rs_block_count)));
            state->rs_block_start += state->rs_block_count;
            state->rs_block_count = 0;

            /* exchange buffers queue */
            if (state->rs_buffers_handler == state->rs_buffers_handler_ptr) {
                Assert(state->rs_buffers_queue == state->rs_buffers_queue_ptr);
                state->rs_buffers_handler = state->rs_buffers_handler_ptr + REWRITE_BUFFERS_QUEUE_COUNT;
                state->rs_buffers_queue = state->rs_buffers_queue_ptr + BLCKSZ * REWRITE_BUFFERS_QUEUE_COUNT;
            } else {
                Assert(state->rs_buffers_queue != state->rs_buffers_queue_ptr);
                state->rs_buffers_handler = state->rs_buffers_handler_ptr;
                state->rs_buffers_queue = state->rs_buffers_queue_ptr;
            }
            /* check buffer state */
            for (int i = 0; i < REWRITE_BUFFERS_QUEUE_COUNT; i++) {
                CheckIOState((char *)(&(state->rs_buffers_handler[i])));
                ereport(DEBUG1,
                        (errmodule(MOD_ADIO),
                         errmsg("rewrite_flush_page, CheckIOState, flags(%lu)",
                                (pg_atomic_read_u64(&state->rs_buffers_handler[i].state) & BUF_FLAG_MASK))));
            }
        }
    } else {
#endif
        smgrextend(state->rs_new_rel->rd_smgr, MAIN_FORKNUM, state->rs_blockno, (char *)page, true);
#ifndef ENABLE_LITE_MODE
    }
#endif
    return;
}

/*
 * @Description:  rewrite flush page
 * @Param[IN] state: RewriteState
 * @See also:
 */
static void rewrite_end_flush_page(RewriteState state)
{
#ifndef ENABLE_LITE_MODE
    /* check aio is ready, for init db in single mode, no aio thread */
    if (AioCompltrIsReady() && g_instance.attr.attr_storage.enable_adio_function) {
        if (state->rs_block_count > 0) {
            rewrite_page_list_write(state);
            state->rs_block_start += state->rs_block_count;
            state->rs_block_count = 0;
        }
        for (int i = 0; i < REWRITE_BUFFERS_QUEUE_COUNT * 2; i++) {
            CheckIOState((char *)(&(state->rs_buffers_handler_ptr[i])));
            ereport(DEBUG1, (errmodule(MOD_ADIO),
                             errmsg("rewrite_end_flush_page, CheckIOState, flags(%lu)",
                                    (pg_atomic_read_u64(&state->rs_buffers_handler[i].state) & BUF_FLAG_MASK))));
        }
    }
#endif
}

/*
 * Insert a tuple to the new relation.	This has to track heap_insert
 * and its subsidiary functions!
 *
 * t_self of the tuple is set to the new TID of the tuple. If t_ctid of the
 * tuple is invalid on entry, it's replaced with the new TID as well (in
 * the inserted data only, not in the caller's copy).
 */
static void raw_heap_insert(RewriteState state, HeapTuple tup)
{
    Page page = state->rs_buffer;
    Size page_free_space, save_free_space;
    Size len;
    OffsetNumber newoff;
    HeapTuple heaptup;
    TransactionId xmin, xmax;

    if (tup != NULL)
        Assert(TUPLE_IS_HEAP_TUPLE(tup));
    else {
        ereport(DEBUG5, (errmodule(MOD_TBLSPC), errmsg("tuple is null")));
        return;
    }

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     *
     * Note: below this point, heaptup is the data we actually intend to store
     * into the relation; tup is the caller's original untoasted data.
     */
    if (state->rs_new_rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
        /* toast table entries should never be recursively toasted */
        Assert(!HeapTupleHasExternal(tup));
        heaptup = tup;
    } else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
        heaptup = toast_insert_or_update(state->rs_new_rel, tup, NULL,
                                         HEAP_INSERT_SKIP_FSM | (state->rs_use_wal ? 0 : HEAP_INSERT_SKIP_WAL), NULL);
    else
        heaptup = tup;

    len = MAXALIGN(heaptup->t_len); /* be conservative */
    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    if (len > MaxHeapTupleSize)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("row is too big: size %lu, maximum size %lu",
                                                                 (unsigned long)len, (unsigned long)MaxHeapTupleSize)));

    /* Compute desired extra freespace due to fillfactor option */
    save_free_space = RelationGetTargetPageFreeSpace(state->rs_new_rel, HEAP_DEFAULT_FILLFACTOR);

    /* Now we can check to see if there's enough free space already. */
    if (state->rs_buffer_valid) {
        page_free_space = PageGetHeapFreeSpace(page);
        if (len + save_free_space > page_free_space) {
            rewrite_write_one_page(state, page);
            state->rs_blockno++;
            state->rs_buffer_valid = false;
        }
    }

    if (!state->rs_buffer_valid) {
        HeapPageHeader phdr = (HeapPageHeader)page;
        /* Initialize a new empty page */
        PageInit(page, BLCKSZ, 0, true);
        phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
        phdr->pd_multi_base = 0;
        state->rs_buffer_valid = true;
        const char* algo = RelationGetAlgo(state->rs_new_rel);
        if (RelationisEncryptEnable(state->rs_new_rel) || (algo && *algo != '\0')) {
            /* 
             * For the reason of saving TdeInfo,
             * we need to move the pointer(pd_special) forward by the length of TdeInfo.
             */
            phdr->pd_upper -= sizeof(TdePageInfo);
            phdr->pd_special -= sizeof(TdePageInfo);
            PageSetTDE(page);
        }
    }

    xmin = HeapTupleGetRawXmin(heaptup);
    xmax = HeapTupleGetRawXmax(heaptup);
    rewrite_page_prepare_for_xid(page, xmin, false);
    (void)rewrite_page_prepare_for_xid(page, xmax, (heaptup->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ? true : false);

    HeapTupleCopyBaseFromPage(heaptup, page);
    HeapTupleSetXmin(heaptup, xmin);
    HeapTupleSetXmax(heaptup, xmax);

    /* And now we can insert the tuple into the page */
    newoff = PageAddItem(page, (Item)heaptup->t_data, heaptup->t_len, InvalidOffsetNumber, false, true);
    if (newoff == InvalidOffsetNumber)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("failed to add tuple")));

    /* Update caller's t_self to the actual position where it was stored */
    ItemPointerSet(&(tup->t_self), state->rs_blockno, newoff);

    /*
     * Insert the correct position into CTID of the stored tuple, too, if the
     * caller didn't supply a valid CTID.
     */
    if (!ItemPointerIsValid(&tup->t_data->t_ctid)) {
        ItemId newitemid;
        HeapTupleHeader onpage_tup;

        newitemid = PageGetItemId(page, newoff);
        onpage_tup = (HeapTupleHeader)PageGetItem(page, newitemid);

        onpage_tup->t_ctid = tup->t_self;
    }

    /* If heaptup is a private copy, release it. */
    if (heaptup != tup)
        heap_freetuple(heaptup);
}

/*
 * Insert a utuple to the new relation.  This has to track UHeapInsert
 * and its subsidiary functions!
 *
 * t_self of the tuple is set to the new TID of the tuple.
 */
static void RawUHeapInsert(RewriteState state, UHeapTuple tup)
{
    Page page = state->rs_buffer;
    Size pageFreeSpace, saveFreeSpace;
    Size len;
    OffsetNumber newoff;
    UHeapTuple uheaptup = NULL;

    if (tup != NULL)
        Assert(tup->tupTableType == UHEAP_TUPLE);

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     *
     * Note: below this point, UHeaptup is the data we actually intend to store
     * into the relation; tup is the caller's original untoasted data.
     */
    if (state->rs_new_rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
        /* toast table entries should never be recursively toasted */
        Assert(!UHeapTupleHasExternal(tup));
        uheaptup = tup;
    } else if (UHeapTupleHasExternal(tup) || tup->disk_tuple_size > UTOAST_TUPLE_THRESHOLD) {
        uheaptup = UHeapToastInsertOrUpdate(state->rs_new_rel, tup, NULL,
            UHEAP_INSERT_SKIP_FSM | (state->rs_use_wal ? 0 : UHEAP_INSERT_SKIP_WAL));
    } else {
        uheaptup = tup;
    }
    len = MAXALIGN(uheaptup->disk_tuple_size); /* be conservative */
    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    if (len > MaxUHeapTupleSize(state->rs_new_rel)) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("row is too big: size %lu, maximum size %lu",
            (unsigned long)len, (unsigned long)MaxUHeapTupleSize(state->rs_new_rel))));
    }

    /* Compute desired extra freespace due to fillfactor option */
    saveFreeSpace = RelationGetTargetPageFreeSpace(state->rs_new_rel, HEAP_DEFAULT_FILLFACTOR);

    /* Now we can check to see if there's enough free space already. */
    if (state->rs_buffer_valid) {
        pageFreeSpace = PageGetUHeapFreeSpace(page);
        if (len + saveFreeSpace > pageFreeSpace) {
            /* Doesn't fit, so write out the existing page */

            /* check tablespace size limitation when extending new file. */
            STORAGE_SPACE_OPERATION(state->rs_new_rel, BLCKSZ);

            /* XLOG stuff */
            if (state->rs_use_wal) {
                log_newpage(&state->rs_new_rel->rd_node, MAIN_FORKNUM, state->rs_blockno, page, true);
            }

            /*
             * Now write the page. We say isTemp = true even if it's not a
             * temp table, because there's no need for smgr to schedule an
             * fsync for this write; we'll do it ourselves in
             * end_heap_rewrite.
             */
            RelationOpenSmgr(state->rs_new_rel);

            PageSetChecksumInplace(page, state->rs_blockno);

            rewrite_flush_page(state, page);

            state->rs_blockno++;
            state->rs_buffer_valid = false;
        }
    }

    if (!state->rs_buffer_valid) {
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        /* Initialize a new empty page */
        UPageInit<UPAGE_HEAP>(page, BLCKSZ, UHEAP_SPECIAL_SIZE, RelationGetInitTd(state->rs_new_rel));
        uheappage->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
        uheappage->pd_multi_base = 0;
        state->rs_buffer_valid = true;
    }

    /* And now we can insert the tuple into the page */
    UHeapBufferPage bufpage = {InvalidBuffer, page};
    newoff = UPageAddItem(state->rs_new_rel, &bufpage, (Item)uheaptup->disk_tuple, uheaptup->disk_tuple_size,
        InvalidOffsetNumber, false);
    if (newoff == InvalidOffsetNumber) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("failed to add tuple")));
    }

    /* Update caller's t_self to the actual position where it was stored */
    ItemPointerSet(&(tup->ctid), state->rs_blockno, newoff);

    /* If uheaptup is a private copy, release it. */
    if (uheaptup != tup) {
        UHeapFreeTuple(uheaptup);
    }
}
