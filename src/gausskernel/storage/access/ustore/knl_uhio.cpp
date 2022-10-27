/* -------------------------------------------------------------------------
 *
 * knl_uhio.cpp
 * I/O operations of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uhio.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hio.h"
#include "access/xact.h"
#include "knl/knl_guc.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "access/visibilitymap.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_uhio.h"

static inline int CalcMaxBlocksToScan(Relation relation)
{
    Assert(relation->pgstat_info);
    /* Max(1, (free_pct^2 * prune_pct^2 * maxSearchLength) */
    int maxBlocks = relation->pgstat_info->t_freeRatio * relation->pgstat_info->t_freeRatio *
        relation->pgstat_info->t_pruneSuccessRatio * relation->pgstat_info->t_pruneSuccessRatio *
        u_sess->attr.attr_storage.umax_search_length_for_prune;

    return Max(1, maxBlocks);
}

Buffer RelationGetBufferForUTuple(Relation relation, Size len, Buffer otherBuffer, int options, BulkInsertState bistate)
{
    bool useFsm = !(options & UHEAP_INSERT_SKIP_FSM);
    bool forceExtend = (options & UHEAP_INSERT_EXTEND);
    Buffer buffer = InvalidBuffer;
    Page page;
    Size pageFreeSpace = 0;
    Size saveFreeSpace = 0;
    BlockNumber     targetBlock;
    BlockNumber     otherBlock;
    bool            needLock = false;

    /*
     * Blocks that extended one by one are different from bulk-extend blocks, and
     * are not recorded into FSM. As its creator session close this realtion, they
     * can not be used by any other body. It is especially obvious for partition
     * bulk insert. Here, if no available found in FSM, we check the last block to
     * reuse the 'leaked free space' mentioned earlier.
     */
    bool test_last_block = false;

    len = SHORTALIGN(len);
    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    if (len > MaxUHeapTupleSize(relation)) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("row is too big: size %zu, maximum size %zu", len, MaxUHeapTupleSize(relation))));
    }

    /* Compute desired extra freespace due to fillfactor option */
    saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    if (otherBuffer != InvalidBuffer) {
        otherBlock = BufferGetBlockNumber(otherBuffer);
    } else {
        otherBlock = InvalidBlockNumber; /* just to keep compiler quiet */
    }

    if ((NULL != bistate) && BufferIsValid(bistate->current_buf)) {
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
     * on, as cached in the BulkInsertState or relcache entry.  If that
     * doesn't work, we ask the Free Space Map to locate a suitable page.
     * Since the FSM's info might be out of date, we have to be prepared to
     * loop around and retry multiple times. (To insure this isn't an infinite
     * loop, we must update the FSM with the correct amount of free space on
     * each page that proves not to be suitable.)  If the FSM has no record of
     * a page with enough free space, we give up and extend the relation.
     *
     * When useFsm is false, we either put the tuple onto the existing target
     * page or extend the relation.
     */
    if (len + saveFreeSpace > MaxUHeapTupleSize(relation)) {
        /* can't fit, don't bother asking FSM */
        targetBlock = InvalidBlockNumber;
        useFsm = false;
    } else if (bistate && bistate->current_buf != InvalidBuffer) {
        targetBlock = BufferGetBlockNumber(bistate->current_buf);
    } else {
        targetBlock = RelationGetTargetBlock(relation);
    }

    if (unlikely(forceExtend)) {
        targetBlock = InvalidBlockNumber;
    } else if (targetBlock == InvalidBlockNumber && useFsm) {
        /*
         * We have no cached target page, so ask the FSM for an initial
         * target.
         */
        targetBlock = GetPageWithFreeSpace(relation, len + saveFreeSpace);
        /*
         * If the FSM knows nothing of the rel, try the last page before we
         * give up and extend.  This avoids one-tuple-per-page syndrome during
         * bootstrapping or in a recently-started system.
         */
        if (targetBlock == InvalidBlockNumber) {
            BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
            if (nblocks > 0) {
                targetBlock = nblocks - 1;
            }
        }
    }

loop:
    while (targetBlock != InvalidBlockNumber) {
        /*
         * Read and exclusive-lock the target block, as well as the other
         * block if one was given, taking suitable care with lock ordering and
         * the possibility they are the same block.
         *
         * If the page-level all-visible flag is set, caller will need to
         * clear both that and the corresponding visibility map bit.  However,
         * by the time we return, we'll have x-locked the buffer, and we don't
         * want to do any I/O while in that state.  So we check the bit here
         * before taking the lock, and pin the page if it appears necessary.
         * Checking without the lock creates a risk of getting the wrong
         * answer, so we'll have to recheck after acquiring the lock.
         */
        if (otherBuffer == InvalidBuffer) {
            /* easy case */
            buffer = ReadBufferBI(relation, targetBlock, RBM_NORMAL, bistate);
            if (!TryLockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE, !test_last_block)) {
                Assert(test_last_block);
                ReleaseBuffer(buffer);

                /* someone is using this block, give up and extend. */
                break;
            }
        } else if (otherBlock == targetBlock) {
            buffer = otherBuffer;
            /* also easy case */
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        } else if (otherBlock < targetBlock) {
            /* lock other buffer first */
            buffer = ReadBuffer(relation, targetBlock);
            LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        } else {
            /* lock target buffer first */
            buffer = ReadBuffer(relation, targetBlock);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
        }

        /*
         * Now we can check to see if there's enough free space here. If
         * so, we're done.
         */
        page = BufferGetPage(buffer);

        /*
         * Note that if we get a new Page here, we're not allowed to initialize it
         * because we are not the one that created it. If we get a new Page,
         * that means some other thread created it but we grabbed the buffer lock
         * before the creating thread and that thread is expecting to initialize
         * the Page on its own, once it grabs the buffer lock.
         * See the PageIsNew() check later in this function.
         * So if we get a new Page, we expect to fail the free space check below
         * and loop again.
         */

        pageFreeSpace = PageGetUHeapFreeSpace(page);
        if (len + saveFreeSpace <= pageFreeSpace) {
            /* use this page as future insert target, too */
            RelationSetTargetBlock(relation, targetBlock);
            return buffer;
        }

        /*
         * Not enough space, so we must give up our page locks
         * and pin (if any) and prepare to look elsewhere.  We don't care
         * which order we unlock the two buffers in, so this can be slightly
         * simpler than the code above.
         */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (otherBuffer == InvalidBuffer)
            ReleaseBuffer(buffer);
        else if (otherBlock != targetBlock) {
            LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
        }

        /* Without FSM, always fall out of the loop and extend */
        if (!useFsm) {
            break;
        }

        /*
         * Update FSM as to condition of this page, and ask for another page
         * to try.
         */
        targetBlock = RecordAndGetPageWithFreeSpace(relation, targetBlock, pageFreeSpace, len + saveFreeSpace);

        /*
         * If the FSM knows nothing of the rel, try the last page before we
         * give up and extend. This's intend to use pages that are extended
         * one by one and not recorded in FSM as possible.
         *
         * The best is to record all pages into FSM using bulk-extend in later.
         */
        if (targetBlock == InvalidBlockNumber && !test_last_block && otherBuffer == InvalidBuffer) {
            BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
            if (nblocks > 0) {
                targetBlock = nblocks - 1;
            }
            test_last_block = true;
        }
    }

    /*
     * See if we can prune an existing block before extending the relation.
     */
    if (useFsm && !forceExtend) {
        targetBlock = RelationPruneOptional(relation, len + saveFreeSpace);
        if (targetBlock != InvalidBlockNumber) {
            goto loop;
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
    needLock = !RELATION_IS_LOCAL(relation);
    /*
     * If we need the lock but are not able to acquire it immediately, we'll
     * consider extending the relation by multiple blocks at a time to manage
     * contention on the relation extension lock.  However, this only makes
     * sense if we're using the FSM; otherwise, there's no point.
     */
    if (needLock) {
        if (!useFsm) {
            LockRelationForExtension(relation, ExclusiveLock);
        } else if (!ConditionalLockRelationForExtension(relation, ExclusiveLock)) {
            /* Couldn't get the lock immediately; wait for it. */
            LockRelationForExtension(relation, ExclusiveLock);

            /*
             * Check if some other backend has extended a block for us while
             * we were waiting on the lock.
             */
            targetBlock = GetPageWithFreeSpace(relation, len + saveFreeSpace);
            /*
             * If some other waiter has already extended the relation, we
             * don't need to do so; just use the existing freespace.
             */
            if (targetBlock != InvalidBlockNumber) {
                UnlockRelationForExtension(relation, ExclusiveLock);
                goto loop;
            }

            /* Time to bulk-extend. */
            RelationAddExtraBlocks(relation, bistate);
        }
    }

    /*
     * In addition to whatever extension we performed above, we always add at
     * least one block to satisfy our own request.
     *
     * XXX This does a rather expensive lseek, but at the moment it is the
     * only way to accurately determine how many blocks are in a relation.  Is
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
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("page %u of relation \"%s\" should be empty but is not", BufferGetBlockNumber(buffer),
                               RelationGetRelationName(relation))));
    }
    /*
     * It is possible that by the time we added new block to the relation,
     * another thread initialized it from RelationPruneBlockAndReturn().
     * We used to treat such condition as error in the earlier code but now
     * since we have two initializers, it seems to be valid that a new page
     * is initialized by the time the block creator thread could get the
     * buffer lock.
     */
    if (relation->rd_rel->relkind == RELKIND_TOASTVALUE) {
        UPageInit<UPAGE_TOAST>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE);
    } else {
        UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE, RelationGetInitTd(relation));
    }
    UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
    uheappage->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    uheappage->pd_multi_base = 0;
    MarkBufferDirty(buffer);
    /*
     * Release the file-extension lock; it's now OK for someone else to extend
     * the relation some more.
     */
    if (needLock) {
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
    if (otherBuffer != InvalidBuffer) {
        Assert(otherBuffer != buffer);

        if (unlikely(!ConditionalLockBuffer(otherBuffer))) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Because the buffer was unlocked for a while, it's possible,
             * although unlikely, that the page was filled. If so, just retry
             * from start.
             */
            if (len > PageGetUHeapFreeSpace(page)) {
                LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK);
                UnlockReleaseBuffer(buffer);

                goto loop;
            }
        }
    }

    if (len > PageGetUHeapFreeSpace(page)) {
        /* We should not get here given the test at the top */
        elog(PANIC, "tuple is too big: tuple size %zu, page free size %zu", len, PageGetUHeapFreeSpace(page));
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
 * Check the in-memory global statistics if relation has some dead tuples to prune.
 * If so, get a block to prune from a global hash.
 */
BlockNumber RelationPruneOptional(Relation relation, Size required_size)
{
    BlockNumber result = InvalidBlockNumber;
    bool fetched = false;
    PgStat_StartBlockTableEntry *startBlockEntry = NULL;
    int64 reltuples = 0;
    float4 threshold = 0;
    int64 deadtuples = 0;
    int64 livetuples = 0;
    int maxSearchLength = 0;
    BlockNumber nextBlock = InvalidBlockNumber;
    PgStat_TableStatus *pgStatInfo = relation->pgstat_info;

    if (pgStatInfo == NULL) {
        goto done;
    }

start:
    /*
     * Check if we have a starting block already. If so, read the latest
     * value and start pruning there.
     */
    if (pgStatInfo->startBlockArray != NULL) {
        /* Temporarily tell other backends we are working on this subset.
         * pg_atomic_exchange_u32 should return the old value.
        */
        uint32 index = pgStatInfo->startBlockIndex;
        BlockNumber startBlkno = pg_atomic_exchange_u32(&pgStatInfo->startBlockArray[index],
                                                        InvalidBlockNumber);
        /* Someone else is working on this subset. Avoid contention and move on. */
        if (startBlkno == InvalidBlockNumber) {
            goto done;
        }

        maxSearchLength = CalcMaxBlocksToScan(relation);

        result = RelationPruneBlockAndReturn(relation, startBlkno, maxSearchLength, required_size, &nextBlock);
        /* failed to prune a block, check another subset next time */
        if (result == InvalidBlockNumber) {
            pgStatInfo->startBlockIndex = (pgStatInfo->startBlockIndex + 1) % START_BLOCK_ARRAY_SIZE;
        }

        Assert(nextBlock != InvalidBlockNumber);
        pg_atomic_exchange_u32(&pgStatInfo->startBlockArray[index], nextBlock);
        goto done;
    }

    /*
     * Now load the in-memory statistics for this table if not yet done
    */
    if (!pgStatInfo->t_globalStatChecked) {
        PgStat_StatTabEntry tabentry;
        fetched = GetTableGstats(relation->rd_rel->relisshared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId,
            RelationGetRelid(relation), RelationIsPartition(relation) ? relation->parentId : InvalidOid, &tabentry);

        pgStatInfo->t_globalStatChecked = true;

        if (!fetched) {
            goto done;
        }

        /*
         * Okay we found this table in the in-memory statistics hash.
         * Now check if there are dead tuples to prune.
         * Nice to have: More flexible to use reloptions values not the autovacuum one
         */
        livetuples = tabentry.n_live_tuples;
        threshold = (float4)u_sess->attr.attr_storage.autovacuum_vac_thresh +
            (u_sess->attr.attr_storage.autovacuum_vac_scale * livetuples);
        deadtuples = tabentry.n_dead_tuples;
        reltuples = livetuples + deadtuples;

        /* Not enough dead tuples to prune */
        if ((float4)deadtuples < threshold) {
            Assert(pgStatInfo->startBlockArray == NULL);
            goto done;
        }

        if (reltuples > 0) {        /* just in case */
            pgStatInfo->t_freeRatio = (float4) deadtuples / (float4) reltuples;
        }

        pgStatInfo->t_pruneSuccessRatio = (tabentry.total_prune_cnt < 100) ?
            1 :
            (float4)tabentry.success_prune_cnt / (float4)tabentry.total_prune_cnt;

        /*
         * Grab the starting block for pruning.
         */
        PgStat_StartBlockTableKey tabkey;
        tabkey.dbid = u_sess->proc_cxt.MyDatabaseId;
        tabkey.relid = RelationGetRelid(relation);
        tabkey.parentid = RelationIsPartition(relation) ? relation->parentId : InvalidOid;
        startBlockEntry = GetStartBlockHashEntry(&tabkey);
        Assert(startBlockEntry);

        pgStatInfo->startBlockIndex = GetTopTransactionId() % START_BLOCK_ARRAY_SIZE;
        pgStatInfo->startBlockArray = startBlockEntry->starting_blocks;

        goto start;
    }

done:
    return result;
}

BlockNumber RelationPruneBlockAndReturn(Relation relation, BlockNumber start_block, BlockNumber max_blocks_to_scan,
    Size required_size, BlockNumber *next_block)
{
    bool pruned = false;
    Size freespace = 0;
    Page page;
    TransactionId fxid = GetTopTransactionId();
    Buffer buffer;
    BlockNumber result = InvalidBlockNumber;
    BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
    BlockNumber blkno = start_block;
    BlockNumber scannedBlocks = 0;
    BlockNumber pruneTryCnt = 0;
    *next_block = start_block;
    PgStat_TableStatus *pgStatInfo = relation->pgstat_info;

    /* Handle the case of relation truncation */
    if (nblocks == 0) {
        return InvalidBlockNumber;
    }

    /* Handle the case when nblocks < START_BLOCK_ARRAY_SIZE */
    if (START_BLOCK_ARRAY_SIZE >= nblocks) {
        pgStatInfo->startBlockIndex = fxid % nblocks;
    }

    BlockNumber initBlock = pgStatInfo->startBlockIndex;

    while (scannedBlocks < max_blocks_to_scan) {
        blkno = (blkno >= nblocks) ? initBlock : blkno;

        buffer = ReadBuffer(relation, blkno);
        /*
         * We call page pruning in Insert-Update-Delete (and Scan).
         * So if we can't grab the buffer lock and this page is prunable then
         * the backend currently holding the lock could potentially prune it,
         * hence we do a conditional lock here.
         */
        if (!ConditionalLockBuffer(buffer)) {
            ReleaseBuffer(buffer);
            goto next;
        }

        pruneTryCnt++;
        *next_block = blkno;
        page = BufferGetPage(buffer);
        /* This logic is similar to LazyScanUHeap() */
        if (PageIsNew(page)) {
            /*
             * An all-zeros page could be left over if a backend extends the
             * relation but crashes before initializing the page, or when
             * bulk-extending the relation (which creates a number of empty
             * pages at the tail end of the relation, but enters them into the
             * FSM)Reclaim such pages for use.
             */

            /*
             * Perform checking of FSM after releasing lock, the fsm is
             * approximate, after all.
             */
            if (relation->rd_rel->relkind == RELKIND_TOASTVALUE) {
                UPageInit<UPAGE_TOAST>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE);
            } else {
                UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE,
                    RelationGetInitTd(relation));
            }
            UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
            uheappage->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
            uheappage->pd_multi_base = 0;
            MarkBufferDirty(buffer);
            UnlockReleaseBuffer(buffer);

            if (GetRecordedFreeSpace(relation, blkno) == 0) {
                freespace = BufferGetPageSize(buffer) - SizeOfUHeapPageHeaderData;
            }
            if (freespace > 0) {
                RecordPageWithFreeSpace(relation, blkno, freespace);
            }

            result = blkno;
            break;
        }

        freespace = PageGetUHeapFreeSpace(page);
        if (UPageIsEmpty((UHeapPageHeaderData *)page) || required_size <= freespace) {
            UnlockReleaseBuffer(buffer);
            RecordPageWithFreeSpace(relation, blkno, freespace);
            result = blkno;
            break;
        }

        /* Done with the easy cases, we try to prune it now */
        pruned = UHeapPagePruneOpt(relation, buffer, InvalidOffsetNumber, 0);
        freespace = PageGetUHeapFreeSpace(page);
        UnlockReleaseBuffer(buffer);

        if (pruned) {
            RecordPageWithFreeSpace(relation, blkno, freespace);
            if (required_size <= freespace) {
                result = blkno;
                break;
            }
        }
    next:
        scannedBlocks++;
        blkno += START_BLOCK_ARRAY_SIZE;
    }

    /* Move the next_block pointer */
    if (result != InvalidBlockNumber) {
        *next_block =
            ((*next_block + START_BLOCK_ARRAY_SIZE) >= nblocks) ? initBlock : (*next_block + START_BLOCK_ARRAY_SIZE);
    }

    if (pruneTryCnt) {
        Oid statFlag = RelationIsPartitionOfSubPartitionTable(relation) ? partid_get_parentid(relation->parentId) :
                                                                          relation->parentId;
        PgstatReportPrunestat(RelationGetRelid(relation), statFlag, relation->rd_rel->relisshared,
            pruneTryCnt, (result != InvalidBlockNumber) ? 1 : 0);
    }

    return result;
}
