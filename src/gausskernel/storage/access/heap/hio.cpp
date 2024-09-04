/* -------------------------------------------------------------------------
 *
 * hio.cpp
 *	  openGauss heap access method input/output code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/heap/hio.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/ustore/knl_upage.h"
#include "commands/tablespace.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

/*
 * RelationPutHeapTuple - place tuple at specified page
 *
 * !!! EREPORT(ERROR) IS DISALLOWED HERE !!!  Must PANIC on failure!!!
 *
 * Note - caller must hold BUFFER_LOCK_EXCLUSIVE on the buffer.
 */
void RelationPutHeapTuple(Relation relation, Buffer buffer, HeapTuple tuple, TransactionId xid)
{
    Page page_header;
    OffsetNumber offnum;
    ItemId item_id;
    Item item;

    /* Add the tuple to the page */
    page_header = BufferGetPage(buffer);

    tuple->t_data->t_choice.t_heap.t_xmin = NormalTransactionIdToShort(
        ((HeapPageHeader)(page_header))->pd_xid_base, xid);

    offnum = PageAddItem(page_header, (Item)tuple->t_data, tuple->t_len, InvalidOffsetNumber, false, true);
    if (offnum == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("failed to add tuple to page")));

    /* Update tuple->t_self to the actual position where it was stored */
    ItemPointerSet(&(tuple->t_self), BufferGetBlockNumber(buffer), offnum);

    /* Insert the correct position into CTID of the stored tuple, too */
    item_id = PageGetItemId(page_header, offnum);
    item = PageGetItem(page_header, item_id);
    ((HeapTupleHeader)item)->t_ctid = tuple->t_self;
}

/*
 * Read in a buffer, using bulk-insert strategy if bistate isn't NULL.
 */
Buffer ReadBufferBI(Relation relation, BlockNumber target_block, ReadBufferMode mode, BulkInsertState bistate)
{
    Buffer buffer;

    /* If not bulk-insert, exactly like ReadBuffer */
    if (!bistate) {
        return ReadBufferExtended(relation, MAIN_FORKNUM, target_block, mode, NULL);
    }

    /* If we have the desired block already pinned, re-pin and return it */
    if (bistate->current_buf != InvalidBuffer) {
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blknum;

        BufferGetTag(bistate->current_buf, &rnode, &forknum, &blknum);

        /* we have to check relfilenode because of partitional relation; */
        if ((BufferGetBlockNumber(bistate->current_buf) == target_block) &&
            (RelFileNodeEquals(relation->rd_node, rnode))) {
            IncrBufferRefCount(bistate->current_buf);
            return bistate->current_buf;
        }
        /* ... else drop the old buffer */
        ReleaseBuffer(bistate->current_buf);
        bistate->current_buf = InvalidBuffer;
    }

    /* Perform a read using the buffer strategy */
    buffer = ReadBufferExtended(relation, MAIN_FORKNUM, target_block, mode, bistate->strategy);

    /* Save the selected block as target for future inserts */
    IncrBufferRefCount(buffer);
    bistate->current_buf = buffer;

    return buffer;
}

/*
 * Extend a relation by multiple blocks to avoid future contention on the
 * relation extension lock.  Our goal is to pre-extend the relation by an
 * amount which ramps up as the degree of contention ramps up, but limiting
 * the result to some sane overall value.
 */
void CheckRelation(const Relation relation, int* extraBlocks, int lockWaiters)
{
    if (RelationIsIndex(relation)) {
        if (RelationIsBucket(relation)) {
            /* bucket relation need less extra blocks */
            *extraBlocks = Min(4, lockWaiters);
        } else {
            *extraBlocks = Min(8, lockWaiters);
        }
    } else {
        if (RelationIsBucket(relation)) {
               /* bucket relation need less extra blocks */
               *extraBlocks = Min(256, lockWaiters);
           } else {
               *extraBlocks = Min(512, lockWaiters * 20);
           }
    }
}

static void UBtreeAddExtraBlocks(Relation relation, BulkInsertState bistate)
{
    int extraBlocks = 0;
    int lockWaiters = RelationExtensionLockWaiterCount(relation);
    if (lockWaiters <= 0) {
        return;
    }
    CheckRelation(relation, &extraBlocks, lockWaiters);
    while (extraBlocks-- >= 0) {
        /* Ouch - an unnecessary lseek() each time through the loop! */
        Buffer buffer = ReadBufferBI(relation, P_NEW, RBM_NORMAL, bistate);
        /* ubtree don't need to read or write here, and don't use FSM */
        ReleaseBuffer(buffer); /* just release the buffer */
    }
}

void RelationAddExtraBlocks(Relation relation, BulkInsertState bistate)
{
    BlockNumber block_num = InvalidBlockNumber;
    BlockNumber first_block = InvalidBlockNumber;
    int extra_blocks = 0;
    Size freespace = 0;
    bool isuheap = RelationIsUstoreFormat(relation);

    if (RelationIsUstoreIndex(relation)) {
        /* ubtree, use another bypass */
        UBtreeAddExtraBlocks(relation, bistate);
        return;
    }

    /* Use the length of the lock wait queue to judge how much to extend. */
    int lock_waiters = RelationExtensionLockWaiterCount(relation);
    if (lock_waiters <= 0) {
        return;
    }

    /*
     * It might seem like multiplying the number of lock waiters by as much
     * as 20 is too aggressive, but benchmarking revealed that smaller numbers
     * were insufficient.  512 is just an arbitrary cap to prevent pathological
     * results.
     * Index relation tuple is smaller than the heap page, so not need expand
     * too many page at a time.
     */
    CheckRelation(relation, &extra_blocks, lock_waiters);
    while (extra_blocks-- >= 0) {
        /* Ouch - an unnecessary lseek() each time through the loop! */
        Buffer buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, bistate);
        Page page = BufferGetPage(buffer);

        if (!RelationIsIndex(relation)) {
            if (isuheap) {
                UHeapPageHeaderData* uheapPage = (UHeapPageHeaderData*)page;
                UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE,
                    RelationGetInitTd(relation));
                uheapPage->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
                /* Mark the page dirty so we don't lose init information */
                MarkBufferDirty(buffer); 
            } else {
                HeapPageHeader phdr = (HeapPageHeader)page;
                PageInit(page, BufferGetPageSize(buffer), 0, true);
                phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
                phdr->pd_multi_base = 0;
                const char* algo = RelationGetAlgo(relation);
                if (RelationisEncryptEnable(relation) || (algo && *algo != '\0')) {
                    phdr->pd_upper -= sizeof(TdePageInfo);
                    phdr->pd_special -= sizeof(TdePageInfo);
                    PageSetTDE(page);
                }
                MarkBufferDirty(buffer);
            }
        }

        block_num = BufferGetBlockNumber(buffer);
        if (!RelationIsIndex(relation)) {
            freespace = RelationIsUstoreFormat(relation) ? PageGetUHeapFreeSpace(page) : PageGetHeapFreeSpace(page);
        } else {
            freespace = BLCKSZ - 1;
        }
        UnlockReleaseBuffer(buffer);

        /* Remember first block number thus added. */
        if (first_block == InvalidBlockNumber) {
            first_block = block_num;
        }

        /*
         * Immediately update the bottom level of the FSM.  This has a good
         * chance of making this page visible to other concurrently inserting
         * backends, and we want that to happen without delay.
         */
        RecordPageWithFreeSpace(relation, block_num, freespace);
    }

    /*
     * Updating the upper levels of the free space map is too expensive
     * to do for every block, but it's worth doing once at the end to make
     * sure that subsequent insertion activity sees all of those nifty free
     * pages we just inserted.
     *
     * Note that we're using the freespace value that was reported for the
     * last block we added as if it were the freespace value for every block
     * we added.  That's actually true, because they're all equally empty.
     */
    UpdateFreeSpaceMap(relation, first_block, block_num, freespace);
}

/*
 * For each heap page which is all-visible, acquire a pin on the appropriate
 * visibility map page, if we haven't already got one.
 *
 * buffer2 may be InvalidBuffer, if only one buffer is involved.  buffer1
 * must not be InvalidBuffer.  If both buffers are specified, block1 must
 * be less than block2.
 */
static void GetVisibilityMapPins(Relation relation, Buffer buffer1, Buffer buffer2, BlockNumber block1,
                                 BlockNumber block2, Buffer *vmbuffer1, Buffer *vmbuffer2)
{
    bool need_to_pin_buffer1 = false;
    bool need_to_pin_buffer2 = false;

    Assert(BufferIsValid(buffer1));
    Assert(buffer2 == InvalidBuffer || block1 <= block2);

    for (;;) {
        /* Figure out which pins we need but don't have. */
        need_to_pin_buffer1 = PageIsAllVisible(BufferGetPage(buffer1)) && !visibilitymap_pin_ok(block1, *vmbuffer1);
        need_to_pin_buffer2 = buffer2 != InvalidBuffer && PageIsAllVisible(BufferGetPage(buffer2)) &&
                              !visibilitymap_pin_ok(block2, *vmbuffer2);
        if (!need_to_pin_buffer1 && !need_to_pin_buffer2) {
            return;
        }

        /* We must unlock both buffers before doing any I/O. */
        LockBuffer(buffer1, BUFFER_LOCK_UNLOCK);
        if (buffer2 != InvalidBuffer && buffer2 != buffer1) {
            LockBuffer(buffer2, BUFFER_LOCK_UNLOCK);
        }

        /* Get pins. */
        if (need_to_pin_buffer1) {
            visibilitymap_pin(relation, block1, vmbuffer1);
        }
        if (need_to_pin_buffer2) {
            visibilitymap_pin(relation, block2, vmbuffer2);
        }

        /* Relock buffers. */
        LockBuffer(buffer1, BUFFER_LOCK_EXCLUSIVE);
        if (buffer2 != InvalidBuffer && buffer2 != buffer1) {
            LockBuffer(buffer2, BUFFER_LOCK_EXCLUSIVE);
        }

        /*
         * If there are two buffers involved and we pinned just one of them,
         * it's possible that the second one became all-visible while we were
         * busy pinning the first one.	If it looks like that's a possible
         * scenario, we'll need to make a second pass through this loop.
         */
        if (buffer2 == InvalidBuffer || buffer1 == buffer2 || (need_to_pin_buffer1 && need_to_pin_buffer2)) {
            break;
        }
    }
}

/*
 * heap_tuple_len_verifier
 *
 * This routine is used to check whether the length of a tuple is oversize or not.
 */
static void heap_tuple_len_verifier(Size len)
{
    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    if (len > MaxHeapTupleSize) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("row is too big: size %lu, maximum size %lu",
                                                                 (unsigned long)len, (unsigned long)MaxHeapTupleSize)));
    }
}

/*
 * RelationGetBufferForTuple
 *
 *	Returns pinned and exclusive-locked buffer of a page in given relation
 *	with free space >= given len.
 *
 *	If otherBuffer is not InvalidBuffer, then it references a previously
 *	pinned buffer of another page in the same relation; on return, this
 *	buffer will also be exclusive-locked.  (This case is used by heap_update;
 *	the otherBuffer contains the tuple being updated.)
 *
 *	The reason for passing otherBuffer is that if two backends are doing
 *	concurrent heap_update operations, a deadlock could occur if they try
 *	to lock the same two buffers in opposite orders.  To ensure that this
 *	can't happen, we impose the rule that buffers of a relation must be
 *	locked in increasing page number order.  This is most conveniently done
 *	by having RelationGetBufferForTuple lock them both, with suitable care
 *	for ordering.
 *
 *	NOTE: it is unlikely, but not quite impossible, for otherBuffer to be the
 *	same buffer we select for insertion of the new tuple (this could only
 *	happen if space is freed in that page after heap_update finds there's not
 *	enough there).	In that case, the page will be pinned and locked only once.
 *
 *	For the vmbuffer and vmbuffer_other arguments, we avoid deadlock by
 *	locking them only after locking the corresponding heap page, and taking
 *	no further lwlocks while they are locked.
 *
 *	We normally use FSM to help us find free space.  However,
 *	if HEAP_INSERT_SKIP_FSM is specified, we just append a new empty page to
 *	the end of the relation if the tuple won't fit on the current target page.
 *	This can save some cycles when we know the relation is new and doesn't
 *	contain useful amounts of free space.
 *
 *	HEAP_INSERT_SKIP_FSM is also useful for non-WAL-logged additions to a
 *	relation, if the caller holds exclusive lock and is careful to invalidate
 *	relation's smgr_targblock before the first insertion --- that ensures that
 *	all insertions will occur into newly added pages and not be intermixed
 *	with tuples from other transactions.  That way, a crash can't risk losing
 *	any committed data of other transactions.  (See heap_insert's comments
 *	for additional constraints needed for safe usage of this behavior.)
 *
 *	The caller can also provide a BulkInsertState object to optimize many
 *	insertions into the same relation.	This keeps a pin on the current
 *	insertion target page (to save pin/unpin cycles) and also passes a
 *	BULKWRITE buffer selection strategy object to the buffer manager.
 *	Passing NULL for bistate selects the default behavior.
 *
 *	We always try to avoid filling existing pages further than the fillfactor.
 *	This is OK since this routine is not consulted when updating a tuple and
 *	keeping it on the same page, which is the scenario fillfactor is meant
 *	to reserve space for.
 *
 *	ereport(ERROR) is allowed here, so this routine *must* be called
 *	before any (unlogged) changes are made in buffer pool.
 */
Buffer RelationGetBufferForTuple(Relation relation, Size len, Buffer other_buffer, int options, BulkInsertState bistate,
                                 Buffer *vmbuffer, Buffer *vmbuffer_other, BlockNumber end_rel_block)
{
    bool use_fsm = !(options & HEAP_INSERT_SKIP_FSM);
    Buffer buffer = InvalidBuffer;
    Page page;
    Size page_free_space = 0;
    Size save_free_space = 0;
    BlockNumber target_block, other_block;
    bool need_lock = false;
    Size extralen = 0;
    HeapPageHeader phdr;

    /*
     * Blocks that extended one by one are different from bulk-extend blocks, and
     * are not recorded into FSM. As its creator session close this realtion, they
     * can not be used by any other body. It is especially obvious for partition
     * bulk insert. Here, if no available found in FSM, we check the last block to
     * reuse the 'leaked free space' mentioned earlier.
     */
    bool test_last_block = false;

    len = MAXALIGN(len); /* be conservative */

    /* Bulk insert is not supported for updates, only inserts. */
    Assert(other_buffer == InvalidBuffer || !bistate);

    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    heap_tuple_len_verifier(len);

    /* Compute desired extra freespace due to fillfactor option */
    save_free_space = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    if (other_buffer != InvalidBuffer) {
        other_block = BufferGetBlockNumber(other_buffer);
    } else {
        other_block = InvalidBlockNumber; /* just to keep compiler quiet */
    }

    if ((bistate != NULL) && BufferIsValid(bistate->current_buf)) {
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blknum;

        BufferGetTag(bistate->current_buf, &rnode, &forknum, &blknum);

        if (!RelFileNodeEquals(relation->rd_node, rnode)) {
            ReleaseBuffer(bistate->current_buf);
            bistate->current_buf = InvalidBuffer;
        }
    }

    /*
     * We first try to put the tuple on the same page we last inserted a tuple
     * on, as cached in the BulkInsertState or relcache entry.	If that
     * doesn't work, we ask the Free Space Map to locate a suitable page.
     * Since the FSM's info might be out of date, we have to be prepared to
     * loop around and retry multiple times. (To insure this isn't an infinite
     * loop, we must update the FSM with the correct amount of free space on
     * each page that proves not to be suitable.)  If the FSM has no record of
     * a page with enough free space, we give up and extend the relation.
     *
     * When use_fsm is false, we either put the tuple onto the existing target
     * page or extend the relation.
     */
    if (len + save_free_space > MaxHeapTupleSize) {
        /* can't fit, don't bother asking FSM */
        target_block = InvalidBlockNumber;
        use_fsm = false;
    } else if (bistate && bistate->current_buf != InvalidBuffer) {
        target_block = BufferGetBlockNumber(bistate->current_buf);
    } else {
        target_block = RelationGetTargetBlock(relation);
    }

    if (target_block == InvalidBlockNumber && use_fsm) {
        /*
         * We have no cached target page, so ask the FSM for an initial
         * target.
         */
        target_block = GetPageWithFreeSpace(relation, len + save_free_space);
        /*
         * If the FSM knows nothing of the rel, try the last page before we
         * give up and extend.	This avoids one-tuple-per-page syndrome during
         * bootstrapping or in a recently-started system.
         */
        if (target_block == InvalidBlockNumber) {
            BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
            if (nblocks > 0) {
                target_block = nblocks - 1;
            }
        }
    }
    /* When in append mode, cannot use cached block which smaller than rel end block */
    if ((end_rel_block != InvalidBlockNumber) && (target_block < (end_rel_block + 1))) {
        target_block = InvalidBlockNumber;
    }

loop:
    while (target_block != InvalidBlockNumber) {
        /*
         * Read and exclusive-lock the target block, as well as the other
         * block if one was given, taking suitable care with lock ordering and
         * the possibility they are the same block.
         *
         * If the page-level all-visible flag is set, caller will need to
         * clear both that and the corresponding visibility map bit.  However,
         * by the time we return, we'll have x-locked the buffer, and we don't
         * want to do any I/O while in that state.	So we check the bit here
         * before taking the lock, and pin the page if it appears necessary.
         * Checking without the lock creates a risk of getting the wrong
         * answer, so we'll have to recheck after acquiring the lock.
         */
        extralen = 0;
        if (other_buffer == InvalidBuffer) {
            /* easy case */
            buffer = ReadBufferBI(relation, target_block, RBM_NORMAL, bistate);
            if (PageIsAllVisible(BufferGetPage(buffer))) {
                visibilitymap_pin(relation, target_block, vmbuffer);
            }

            if (!TryLockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE, !test_last_block)) {
                Assert(test_last_block);
                ReleaseBuffer(buffer);

                /* someone is using this block, give up and extend a new one. */
                break;
            }
        } else if (other_block == target_block) {
            /* also easy case */
            buffer = other_buffer;
            if (PageIsAllVisible(BufferGetPage(buffer))) {
                visibilitymap_pin(relation, target_block, vmbuffer);
            }
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        } else if (other_block < target_block) {
            /* lock other buffer first */
            buffer = ReadBuffer(relation, target_block);
            if (PageIsAllVisible(BufferGetPage(buffer))) {
                visibilitymap_pin(relation, target_block, vmbuffer);
            }
            LockBuffer(other_buffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        } else {
            /* lock target buffer first */
            buffer = ReadBuffer(relation, target_block);
            if (PageIsAllVisible(BufferGetPage(buffer))) {
                visibilitymap_pin(relation, target_block, vmbuffer);
            }
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(other_buffer, BUFFER_LOCK_EXCLUSIVE);
        }

        /*
         * We now have the target page (and the other buffer, if any) pinned
         * and locked.	However, since our initial PageIsAllVisible checks
         * were performed before acquiring the lock, the results might now be
         * out of date, either for the selected victim buffer, or for the
         * other buffer passed by the caller.  In that case, we'll need to
         * give up our locks, go get the pin(s) we failed to get earlier, and
         * re-lock.  That's pretty painful, but hopefully shouldn't happen
         * often.
         *
         * Note that there's a small possibility that we didn't pin the page
         * above but still have the correct page pinned anyway, either because
         * we've already made a previous pass through this loop, or because
         * caller passed us the right page anyway.
         *
         * Note also that it's possible that by the time we get the pin and
         * retake the buffer locks, the visibility map bit will have been
         * cleared by some other backend anyway.  In that case, we'll have
         * done a bit of extra work for no gain, but there's no real harm
         * done.
         */
        if (other_buffer == InvalidBuffer || target_block <= other_block) {
            GetVisibilityMapPins(relation, buffer, other_buffer, target_block, other_block, vmbuffer, vmbuffer_other);
        } else {
            GetVisibilityMapPins(relation, other_buffer, buffer, other_block, target_block, vmbuffer_other, vmbuffer);
        }

        /*
         * Now we can check to see if there's enough free space here. If so,
         * we're done.
         */
        page = BufferGetPage(buffer);
        page_free_space = PageGetHeapFreeSpace(page);
        if (len + save_free_space <= page_free_space) {
            /* use this page as future insert target, too */
            RelationSetTargetBlock(relation, target_block);
            return buffer;
        }

        /*
         * Not enough space, so we must give up our page locks and pin (if
         * any) and prepare to look elsewhere.	We don't care which order we
         * unlock the two buffers in, so this can be slightly simpler than the
         * code above.
         */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (other_buffer == InvalidBuffer) {
            ReleaseBuffer(buffer);
        } else if (other_block != target_block) {
            LockBuffer(other_buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
        }
        /* Without FSM, always fall out of the loop and extend */
        if (!use_fsm) {
            break;
        }

        /*
         * Update FSM as to condition of this page, and ask for another page
         * to try.
         */
        target_block = RecordAndGetPageWithFreeSpace(relation, target_block, page_free_space,
                                                     len + save_free_space + extralen);
        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                         errmsg("RelationGetBufferForTuple, get target block %u from FSM, nblocks in relation is %u",
                                target_block, smgrnblocks(relation->rd_smgr, MAIN_FORKNUM))));

        /*
         * If the FSM knows nothing of the rel, try the last page before we
         * give up and extend. This's intend to use pages that are extended
         * one by one and not recorded in FSM as possible.
         *
         * The best is to record all pages into FSM using bulk-extend in later.
         */
        if (target_block == InvalidBlockNumber && !test_last_block && other_buffer == InvalidBuffer) {
            BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
            if (nblocks > 0) {
                target_block = nblocks - 1;
            }
            test_last_block = true;
        }
    }

    /*
     * Have to extend the relation.
     *
     * We have to use a lock to ensure no one else is extending the rel at the
     * same time, else we will both try to initialize the same new page.  We
     * can skip locking for new or temp relations, however, since no one else
     * could be accessing them.
     */
    need_lock = !RELATION_IS_LOCAL(relation);
    /*
     * If we need the lock but are not able to acquire it immediately, we'll
     * consider extending the relation by multiple blocks at a time to manage
     * contention on the relation extension lock.  However, this only makes
     * sense if we're using the FSM; otherwise, there's no point.
     */
    if (need_lock) {
        if (!use_fsm) {
            LockRelationForExtension(relation, ExclusiveLock);
        } else if (!ConditionalLockRelationForExtension(relation, ExclusiveLock)) {
            /* Couldn't get the lock immediately; wait for it. */
            LockRelationForExtension(relation, ExclusiveLock);
            /*
             * Check if some other backend has extended a block for us while
             * we were waiting on the lock.
             */
            target_block = GetPageWithFreeSpace(relation, len + save_free_space + extralen);
            /*
             * If some other waiter has already extended the relation, we
             * don't need to do so; just use the existing freespace.
             */
            if (target_block != InvalidBlockNumber) {
                UnlockRelationForExtension(relation, ExclusiveLock);
                goto loop;
            }

            /* Time to bulk-extend. */
            RelationAddExtraBlocks(relation, bistate);
        }
    }

    /*
     * In addition to whatever extension we performed above, we always add
     * at least one block to satisfy our own request.
     *
     * XXX This does an lseek - rather expensive - but at the moment it is the
     * only way to accurately determine how many blocks are in a relation.	Is
     * it worth keeping an accurate file length in shared memory someplace,
     * rather than relying on the kernel to do it for us?
     */
    buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, bistate);

    /*
     * We need to initialize the empty new page.  Double-check that it really
     * is empty (this should never happen, but if it does we don't want to
     * risk wiping out valid data).
     */
    page = BufferGetPage(buffer);
    if (!PageIsNew(page)) {
        int elevel = ENABLE_DMS ? PANIC : ERROR;
        ereport(elevel,
            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("page %u of relation \"%s\" should be empty but is not",
            BufferGetBlockNumber(buffer), RelationGetRelationName(relation))));
    }

    phdr = (HeapPageHeader)page;
    PageInit(page, BufferGetPageSize(buffer), 0, true);
    phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    phdr->pd_multi_base = 0;
    const char* algo = RelationGetAlgo(relation);
    if (RelationisEncryptEnable(relation) || (algo && *algo != '\0')) {
        /* 
         * For the reason of saving TdeInfo,
         * we need to move the pointer(pd_special) forward by the length of TdeInfo.
         */
        phdr->pd_upper -= sizeof(TdePageInfo);
        phdr->pd_special -= sizeof(TdePageInfo);
        PageSetTDE(page);
    }
    MarkBufferDirty(buffer);
    /*
     * Release the file-extension lock; it's now OK for someone else to extend
     * the relation some more.	Note that we cannot release this lock before
     * we have buffer lock on the new page, or we risk a race condition
     * against vacuumlazy.c --- see comments therein.
     */
    if (need_lock) {
        UnlockRelationForExtension(relation, ExclusiveLock);
    }

    /*
     * Lock the other buffer. It's guaranteed to be of a lower page number
     * than the new page. To conform with the deadlock prevent rules, we ought
     * to lock otherBuffer first, but that would give other backends a chance
     * to put tuples on our page. To reduce the likelihood of that, attempt to
     * lock the other buffer conditionally, that's very likely to work.
     * Otherwise we need to lock buffers in the correct order, and retry if
     * the space has been used in the mean time.
     *
     * Alternatively, we could acquire the lock on otherBuffer before
     * extending the relation, but that'd require holding the lock while
     * performing IO, which seems worse than an unlikely retry.
     */
    if (other_buffer != InvalidBuffer) {
        Assert(other_buffer != buffer);

        if (unlikely(!ConditionalLockBuffer(other_buffer))) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            LockBuffer(other_buffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Because the buffer was unlocked for a while, it's possible,
             * although unlikely, that the page was filled. If so, just retry
             * from start.
             */
            if (len > PageGetHeapFreeSpace(page)) {
                LockBuffer(other_buffer, BUFFER_LOCK_UNLOCK);
                UnlockReleaseBuffer(buffer);

                goto loop;
            }
        }
    }

    if (len > PageGetHeapFreeSpace(page)) {
        /* We should not get here given the test at the top */
        ereport(PANIC, (errmsg("tuple is too big: size %lu", (unsigned long)len)));
    }

    /*
     * Remember the new page as our target for future insertions.
     *
     * XXX should we enter the new page into the free space map immediately,
     * or just keep it for this backend's exclusive use in the short run
     * (until VACUUM sees it)?	Seems to depend on whether you expect the
     * current backend to make more insertions or not, which is probably a
     * good bet most of the time.  So for now, don't add it to FSM yet.
     */
    RelationSetTargetBlock(relation, BufferGetBlockNumber(buffer));

    return buffer;
}

/*
 * RelationGetNewBufferForBulkInsert
 *
 * this function is similar to RelationGetBufferForTuple, the main difference is as following:
 * For concurrent bulkload all buffers got from this function is new and exclusive.
 */
Buffer RelationGetNewBufferForBulkInsert(Relation relation, Size len, Size dict_size, BulkInsertState bistate)
{
    Buffer buffer;
    Page page;
    bool need_lock = false;
    HeapPageHeader phdr;

    need_lock = !RELATION_IS_LOCAL(relation);
    if (need_lock) {
        LockRelationForExtension(relation, ExclusiveLock);
    }

    len = MAXALIGN(len); /* be conservative */

    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    heap_tuple_len_verifier(len);

    buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, bistate);

    if (need_lock) {
        UnlockRelationForExtension(relation, ExclusiveLock);
    }

    page = BufferGetPage(buffer);
    if (!PageIsNew(page)) {
        int elevel = ENABLE_DMS ? PANIC : ERROR;
        ereport(elevel,
            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("page %u of relation \"%s\" should be empty but is not",
            BufferGetBlockNumber(buffer), RelationGetRelationName(relation))));
    }

    phdr = (HeapPageHeader)page;
    PageInit(page, BufferGetPageSize(buffer), 0, true);
    phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    phdr->pd_multi_base = 0;

    RelationSetTargetBlock(relation, BufferGetBlockNumber(buffer));
    return buffer;
}
