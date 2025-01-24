/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrinsert.cpp
 *  DML functions for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrinsert.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"
#include "storage/predicate.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "catalog/index.h"
#include "catalog/pg_partition_fn.h"

void UBTreeResetWaitTimeForTDSlot();
static TransactionId UBTreePCRCheckUnique(Relation rel, IndexTuple itup, Relation heapRel, Buffer buf, OffsetNumber offset,
    BTScanInsert itup_key, IndexUniqueCheck checkUnique, bool* is_unique, GPIScanDesc gpiScan);
void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack);
static Buffer UBTreePCRSplit(Relation rel, Buffer buf, Buffer cbuf, OffsetNumber firstrightoff, OffsetNumber newitemoff,
    Size newitemsz, IndexTuple newitem, bool newitemonleft, BTScanInsert itup_key,uint8 tdslot, UndoRecPtr urec,
    undo::XlogUndoMeta *meta, bool noinsert);
static void UBTreePCRSplitForTD(Relation rel, Buffer buf, BTScanInsert itup_key, BTStack stack);
static bool UBTreePCRPageAddTuple(Page page, Size itemsize, ItemId iid, IndexTuple itup, OffsetNumber itup_off, 
    bool copyflags, uint8 tdslot, bool isNew = true);
void UBTreePCROrWaitForTDSlot(TransactionId xWait, bool isInsert);
static Buffer UBTreePCRNewRoot(Relation rel, Buffer lbuf, Buffer rbuf);
static OffsetNumber UBTreePCRFindInsertLoc(Relation rel, Buffer *bufptr, OffsetNumber firstlegaloff, BTScanInsert itup_key,
    IndexTuple newtup, bool checkingunique, BTStack stack, Relation heapRel);
bool UBTPCRIsNormalInsert(Relation rel, Buffer& pbuf, OffsetNumber& offnum, IndexTuple itup,
    BTScanInsert itupKye, BTStack stack);
static bool UBTreePCRInsertOnPage(Relation rel, BTScanInsert itup_key, Buffer buf, Buffer cbuf, BTStack stack,
    IndexTuple itup, OffsetNumber newitemoff, bool split_only_page, int tdslot, UndoRecPtr urecptr, 
    undo::XlogUndoMeta *xlum);
static void UBTreePCRDupInsertOnPage(Relation rel, Buffer buf, OffsetNumber offnum, int tdSlot,
    UndoRecPtr urecptr, undo::XlogUndoMeta *xlum);
uint8 PreparePCRDelete(Relation rel, Buffer buf, OffsetNumber offnum, UBTreeUndoInfo undoInfo, TransactionId* minXid, bool* needRetry);


/*
 *	UBTreePCRDoInsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routines, btbuild
 *		and btinsert.  By here, itup is filled in, including the TID.
 *
 *		If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
 *		will allow duplicates.	Otherwise (UNIQUE_CHECK_YES or
 *		UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
 *		For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
 *		don't actually insert.
 *
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL
 *		it must be TRUE if the entry is known unique, else FALSE.
 *		(In the current implementation we'll also return TRUE after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool UBTreePCRDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel)
{
    bool is_unique = false;
    BTScanInsert itupKey;
    BTStack stack = NULL;
    Buffer buf;
    OffsetNumber offset;
    Oid indexHeapRelOid = InvalidOid;
    Relation indexHeapRel = NULL;
    Partition part = NULL;
    Relation partRel = NULL;

    bool checkingunique = (checkUnique != UNIQUE_CHECK_NO);

    WHITEBOX_TEST_STUB("UBTreePCRDoInsert-begin", WhiteboxDefaultErrorEmit);
    if (rel == NULL || rel->rd_rel == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("relation or rd_rel is NULL")));
    }
    /* we need an insertion scan key to do our search, so build one */
    itupKey = UBTreeMakeScanKey(rel, itup);
    GPIScanDesc gpiScan = NULL;

    if (RelationIsGlobalIndex(rel)) {
        Assert(RelationIsPartition(heapRel));
        GPIScanInit(&gpiScan);
        indexHeapRelOid = IndexGetRelation(rel->rd_id, false);
        Assert(OidIsValid(indexHeapRelOid));

        if (indexHeapRelOid == heapRel->grandparentId) {  // For subpartitiontable
            indexHeapRel = relation_open(indexHeapRelOid, AccessShareLock);
            Assert(RelationIsSubPartitioned(indexHeapRel));
            part = partitionOpen(indexHeapRel, heapRel->parentId, AccessShareLock);
            partRel = partitionGetRelation(indexHeapRel, part);
            gpiScan->parentRelation = partRel;
            partitionClose(indexHeapRel, part, AccessShareLock);
        } else { /* gpi of patitioned table */
            gpiScan->parentRelation = relation_open(heapRel->parentId, AccessShareLock);
        }
    }

    if (checkingunique) {
        if (!itupKey->anynullkeys) {
            /* No (heapkeyspace) scantid until uniqueness established */
            itupKey->scantid = NULL;
        } else {
            /*
             * Scan key for new tuple contains NULL key values.  Bypass
             * checkingunique steps.  They are unnecessary because core code
             * considers NULL unequal to every value, including NULL.
             *
             * This optimization avoids O(N^2) behavior within the
             * _bt_findinsertloc() heapkeyspace path when a unique index has a
             * large number of "duplicates" with NULL key values.
             */
            checkingunique = false;
            /* Tuple is unique in the sense that core code cares about */
            Assert(checkUnique != UNIQUE_CHECK_EXISTING);
            is_unique = true;
        }
    }
    UBTreeResetWaitTimeForTDSlot();
    /*
     * It's very common to have an index on an auto-incremented or
     * monotonically increasing value. In such cases, every insertion happens
     * towards the end of the index. We try to optimise that case by caching
     * the right-most leaf of the index. If our cached block is still the
     * rightmost leaf, has enough free space to accommodate a new entry and
     * the insertion key is strictly greater than the first key in this page,
     * then we can safely conclude that the new key will be inserted in the
     * cached block. So we simply search within the cached block and insert the
     * key at the appropriate location. We call it a fastpath.
     *
     * Testing has revealed, though, that the fastpath can result in increased
     * contention on the exclusive-lock on the rightmost leaf page. So we
     * conditionally check if the lock is available. If it's not available then
     * we simply abandon the fastpath and take the regular path. This makes
     * sense because unavailability of the lock also signals that some other
     * backend might be concurrently inserting into the page, thus reducing our
     * chances to finding an insertion place in this page.
     */
top:
    bool fastpath = false;
    offset = InvalidOffsetNumber;

    if (RelationGetTargetBlock(rel) != InvalidBlockNumber) {
        /*
         * Conditionally acquire exclusive lock on the buffer before doing any
         * checks. If we don't get the lock, we simply follow slowpath. If we
         * do get the lock, this ensures that the index state cannot change, as
         * far as the rightmost part of the index is concerned.
         */
        buf = ReadBuffer(rel, RelationGetTargetBlock(rel));
        if (ConditionalLockBuffer(buf)) {
            _bt_checkpage(rel, buf);

            Page page = BufferGetPage(buf);
            UBTPCRPageOpaque lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            Size itemsz = MAXALIGN(IndexTupleSize(itup));

            /*
             * Check if the page is still the rightmost leaf page, has enough
             * free space to accommodate the new tuple, no split is in progress
             * and the scankey is greater than or equal to the first key on the
             * page.
             */
            if (P_ISLEAF(lpageop) && P_RIGHTMOST(lpageop) && !P_INCOMPLETE_SPLIT(lpageop) && !P_IGNORE(lpageop) &&
                (PageGetFreeSpace(page) > itemsz) && UBTreePCRPageGetMaxOffsetNumber(page) >= P_FIRSTDATAKEY(lpageop) &&
                UBTreePCRCompare(rel, itupKey, page, P_FIRSTDATAKEY(lpageop)) > 0) {
                fastpath = true;
            } else {
                _bt_relbuf(rel, buf);

                /*
                 * Something did not workout. Just forget about the cached
                 * block and follow the normal path. It might be set again if
                 * the conditions are favourble.
                 */
                RelationSetTargetBlock(rel, InvalidBlockNumber);
            }
        } else {
            ReleaseBuffer(buf);

            /*
             * If someone's holding a lock, it's likely to change anyway,
             * so don't try again until we get an updated rightmost leaf.
             */
            RelationSetTargetBlock(rel, InvalidBlockNumber);
        }
    }

    if (!fastpath) {
        /* find the first page containing this key, buffer returned by _bt_search() is locked in exclusive mode */
        stack = UBTreePCRSearch(rel, itupKey, &buf, BT_WRITE, true);
    }

    WHITEBOX_TEST_STUB("UBTreePCRDoInsert-found", WhiteboxDefaultErrorEmit);

    /*
     * If we're not allowing duplicates, make sure the key isn't already in
     * the index.
     *
    * NOTE: obviously, _bt_check_unique can only detect keys that are already
     * in the index; so it cannot defend against concurrent insertions of the
     * same key.  We protect against that by means of holding a write lock on
     * the first page the value could be on, regardless of the value of its
     * implicit heap TID tiebreaker attribute.  Any other would-be inserter of
     * the same key must acquire a write lock on the same page, so only one
     * would-be inserter can be making the check at one time.  Furthermore,
     * once we are past the check we hold write locks continuously until we
     * have performed our insertion, so no later inserter can fail to see our
     * insertion.  (This requires some care in _bt_findinsertloc.)
     *
     * If we must wait for another xact, we release the lock while waiting,
     * and then must start over completely.
     *
     * For a partial uniqueness check, we don't wait for the other xact. Just
     * let the tuple in and return false for possibly non-unique, or true for
     * definitely unique.
     */
    if (checkingunique) {
        TransactionId xwait;

        offset = UBTreePCRBinarySearch(rel, itupKey, BufferGetPage(buf));
        xwait = UBTreePCRCheckUnique(rel, itup, heapRel, buf, offset, itupKey, checkUnique, &is_unique, gpiScan);
        if (unlikely(TransactionIdIsValid(xwait))) {
            /* Have to wait for the other guy ... */
            _bt_relbuf(rel, buf);
            buf = InvalidBuffer;
            XactLockTableWait(xwait);
            /* start over... */
            if (stack) {
                _bt_freestack(stack);
            }
            goto top;
        }

        /* Uniqueness is established -- restore heap tid as scantid */
        if (itupKey->heapkeyspace) {
            itupKey->scantid = &itup->t_tid;
        }
    }

    bool split = false;
    if (checkUnique != UNIQUE_CHECK_EXISTING) {
        /*
         * The only conflict predicate locking cares about for indexes is when
         * an index tuple insert conflicts with an existing lock.  We don't
         * know the actual page we're going to insert to yet because scantid
         * was not filled in initially, but it's okay to use the "first valid"
         * page instead.  This reasoning also applies to INCLUDE indexes,
         * whose extra attributes are not considered part of the key space.
         */
        CheckForSerializableConflictIn(rel, NULL, buf);
        /* do the insertion */
        offset = UBTreePCRFindInsertLoc(rel, &buf, offset, itupKey, itup, checkingunique, stack, heapRel);
        
        bool isNormalInsert = UBTPCRIsNormalInsert(rel, buf, offset, itup, itupKey, stack);
        Page page = BufferGetPage(buf);

        OffsetNumber off = isNormalInsert ? InvalidOffsetNumber : offset;

        /* reserve td slot */
        uint8 tdSlot = UBTreeInvalidTDSlotId;
        uint8 prevSlot = UBTreeFrozenTDSlotId;
        UBTreeTDData prevTd;
        TransactionId fxid;
        while (true) {
            TransactionId minXid = MaxTransactionId;
            bool needRetry = false;
            fxid = GetTopTransactionId();
            tdSlot = UBTreePageReserveTransactionSlot(rel, buf, fxid, &prevTd, &minXid);
            if (tdSlot == UBTreeInvalidTDSlotId) {
                uint8 tdCount = UBTreePageGetTDSlotCount(page);
                Size freeSpace = PageGetFreeSpace(page);
                if (tdCount < UBTREE_MAX_TD_COUNT && freeSpace < sizeof(UBTreeTDData)) {
                    UBTreePCRSplitForTD(rel, buf, itupKey, stack);
                } else {
                    _bt_relbuf(rel, buf);
                    UBTreePCROrWaitForTDSlot(minXid, true);
                }
                buf = InvalidBuffer;
                if (checkUnique && itupKey->heapkeyspace) {
                    itupKey->scantid = nullptr;
                }
                _bt_freestack(stack);
                goto top;
            }
            
            prevSlot = UBTreeFrozenTDSlotId;
            if (off != InvalidOffsetNumber) {
                ItemId iid = UBTreePCRGetRowPtr(page, off);
                IndexTuple itup = UBTreePCRGetIndexTuple(page, off);
                IndexTupleTrx trx = UBTreePCRGetIndexTupleTrx(itup);
                if (!ItemIdIsDead(iid)) {
                    prevSlot = trx->tdSlot;
                    UBTreePCRHandlePreviousTD(rel, buf, &prevSlot, trx, &needRetry);
                    if (needRetry) {
                        buf = InvalidBuffer;
                        if (checkUnique && itupKey->heapkeyspace) {
                            itupKey->scantid = nullptr;
                        }
                        _bt_freestack(stack);
                        goto top;
                    }
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("UBTree pcr insert conflict, relfilenode[%u], block[%d], offset[%u], prevSlot[%d],"
                        "reserved slot[%d]", rel->rd_node.relNode, BufferGetBlockNumber(buf), off, prevSlot, tdSlot)));
                }
            }
            break;
        }

        /*prepare undo record*/
        undo::XlogUndoMeta xlum;
        UBTreeUndoInfoData undoinfo;
        UndoPersistence persistence = UndoPersistenceForRelation(rel);
        Oid relOid = RelationIsPartition(rel) ? GetBaseRelOidOfParition(rel) : RelationGetRelid(rel);
        Oid partitionOid = RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid;
        bool selfInsert = prevTd.xactid == fxid;
        undoinfo.prev_td_id = prevSlot;
        UndoRecPtr urecPtr;
        urecPtr = UBTreePCRPrepareUndoInsert(relOid, partitionOid, RelationGetRelFileNode(rel), 
            RelationGetRnodeSpace(rel), persistence, GetTopTransactionId(), GetCurrentCommandId(true), 
            prevTd.undoRecPtr, INVALID_UNDO_REC_PTR, InvalidBlockNumber, NULL, &xlum, offset, InvalidBuffer,
            selfInsert, &undoinfo, itup);
        
        if (isNormalInsert) {
            split = UBTreePCRInsertOnPage(rel, itupKey, buf, InvalidBuffer, stack, itup,
                                          offset, false, tdSlot, urecPtr, &xlum);
        } else {
            UBTreePCRDupInsertOnPage(rel, buf, offset, tdSlot, urecPtr, &xlum);
        }
    } else {
        /* just release the buffer */
        _bt_relbuf(rel, buf);
    }

    UHeapResetPreparedUndo();
    if (split) {
        UBTreePCRTryRecycleEmptyPage(rel);
    }
    /* be tidy */
    if (stack) {
        _bt_freestack(stack);
    }
    pfree(itupKey);

    if (gpiScan != NULL) { // means rel switch happened
        if (indexHeapRelOid == heapRel->grandparentId) {  // For subpartitiontable
            releaseDummyRelation(&partRel);
            relation_close(indexHeapRel, AccessShareLock);
        } else {
            relation_close(gpiScan->parentRelation, AccessShareLock);
        }
        GPIScanEnd(gpiScan);
    }

    WHITEBOX_TEST_STUB("UBTreePCRDoInsert-end", WhiteboxDefaultErrorEmit);

    return is_unique;
}

bool UBTreePCRDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex)
{
    bool found = true;
    BTScanInsert itupKey;
    Buffer buf;
    volatile Page page = NULL;
    OffsetNumber offset;
    BTStack stack = NULL;
    TransactionId fxid = GetTopTransactionId();

    /* Create an insertion scan key to search itup. */
    itupKey = UBTreeMakeScanKey(rel, itup);
    UBTreeResetWaitTimeForTDSlot();

DELETE_RETRY:
    /*
     * STEP 1: Look for the first page that may contain this tuple
     */
    stack = UBTreePCRSearch(rel, itupKey, &buf, BT_WRITE, false);

    /*
     * STEP 2: Look for the exact tuple that needs to be deleted.
     * 
     * Note: 1. The index keys, heap tid, and part oid of the found tuple must exactly match those of itup.
     *       2. Retry this step if page has been pruned.
     */ 
    uint8 tdslot = UBTreeInvalidTDSlotId;
    UBTreeUndoInfoData undoInfo;
    while (true) {
        /* Get the offset number of the tuple. */
        offset = UBTreePCRBinarySearch(rel, itupKey, BufferGetPage(buf));

        /* Look for the tuple with same keys, tie and part oid */
        offset = UBTreePCRFindDeleteLoc(rel, &buf, offset, itupKey, itup);
        /* Get the page and disable optimization */
        page = BufferGetPage(buf);
        if (offset == InvalidOffsetNumber) {
            ReportFailedToDelete(rel, buf, itup, "can't find it");
        }
        /* shift xid-base if nece */

        TransactionId minXid = MaxTransactionId;
        bool needRetry = false;
        /* Preparation tdslot */
        tdslot = PreparePCRDelete(rel, buf, offset, &undoInfo, &minXid, &needRetry);         
        if (needRetry) {
            /* in this case we have unlock and release the buf to wait for inprogress xid, thus need to retry */
            goto DELETE_RETRY;
        }
        if (tdslot != UBTreeInvalidTDSlotId) {
            break;
        } else {
            _bt_relbuf(rel, buf);
            UHeapSleepOrWaitForTDSlot(minXid, false);
        }
        buf = InvalidBuffer;
        goto DELETE_RETRY;
    }
    // TODO
    undo::XlogUndoMeta xlum;
    UndoRecPtr urecPtr = INVALID_UNDO_REC_PTR;

    /* Prepare the undo space before acquiring lock */
    UndoPersistence persistence = UndoPersistenceForRelation(rel);
    Oid relOid = RelationIsPartition(rel) ? rel->parentId : RelationGetRelid(rel);
    Oid partitionOid = RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid;

    urecPtr = UBTreePCRPrepareUndoDelete(relOid, partitionOid, RelationGetRelFileNode(rel),
        RelationGetRnodeSpace(rel), persistence, InvalidBuffer, GetTopTransactionId(), GetCurrentCommandId(false), INVALID_UNDO_REC_PTR, 
        itup, InvalidBlockNumber, NULL, &xlum, &undoInfo);

    /* STEP3: mark xmax for the target tuple */
    UBTreePCRPageDel(rel, buf, offset, isRollbackIndex, tdslot, urecPtr, &xlum);

    UHeapResetPreparedUndo();
    /* be tidy */
    pfree(itupKey);
    return found;
}

bool UBTreePCRPagePruneOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos)
{
    return false;
}

bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts)
{
    return false;
}

bool UBTreePCRPruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate, bool isToast)
{
    return false;
}

void UBTreePCRPagePruneExecute(Page page, OffsetNumber* nowdead, int ndead, IndexPruneState* prstate, TransactionId oldest_xmin)
{
    return;
}

void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page)
{
    return;
}

/*
 * UBTreePCRInsertOnPage() -- Insert a tuple on a particular page in the index.
 *
 *      This recursive procedure does the following things
 *
 *      On entry, we must have the correct buffer in which to do the
 *      insertion, and the buffer must be pinned and write-locked.	On return,
 *      we will have dropped both the pin and the lock on the buffer.
 *
 *      When inserting to a non-leaf page, 'cbuf' is the left-sibling of the
 *      page we're inserting the downlink for.  This function will clear the
 *      INCOMPLETE_SPLIT flag on it, and release the buffer.
 *
 *      The locking interactions in this code are critical.  You should
 *      grok Lehman and Yao's paper before making any changes.  In addition,
 *      you need to understand how we disambiguate duplicate keys in this
 *      implementation, in order to be able to find our location using
 *      L&Y "move right" operations.  Since we may insert duplicate user
 *      keys, and since these dups may propagate up the tree, we use the
 *		'afteritem' parameter to position ourselves correctly for the
 *      insertion on internal pages.
 */
static bool UBTreePCRInsertOnPage(Relation rel, BTScanInsert itup_key, Buffer buf, Buffer cbuf, BTStack stack,
    IndexTuple itup, OffsetNumber newitemoff, bool split_only_page, int tdslot, UndoRecPtr urecptr, 
    undo::XlogUndoMeta *xlum)
{
    Page page;
    UBTPCRPageOpaque lpageop;
    OffsetNumber firstright = InvalidOffsetNumber;
    Size itemsz;

    page = BufferGetPage(buf);
    lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    /* child buffer must be given iff inserting on an internal page */
    Assert(P_ISLEAF(lpageop) == !BufferIsValid(cbuf));
    /* tuple must have appropriate number of attributes */
    Assert(!P_ISLEAF(lpageop) ||
           UBTreeTupleGetNAtts(itup, rel) ==
           IndexRelationGetNumberOfAttributes(rel));
    Assert(P_ISLEAF(lpageop) ||
           UBTreeTupleGetNAtts(itup, rel) <=
           IndexRelationGetNumberOfKeyAttributes(rel));

    WHITEBOX_TEST_STUB("UBTreePCRInsertOnPage", WhiteboxDefaultErrorEmit);

    /*
     * Every internal page should have exactly one negative infinity item at
     * all times.  Only _bt_split() and _bt_newroot() should add items that
     * become negative infinity items through truncation, since they're the
     * only routines that allocate new internal pages.
     */
    Assert(P_ISLEAF(lpageop) || newitemoff > P_FIRSTDATAKEY(lpageop));

    /* The caller should've finished any incomplete splits already. */
    if (P_INCOMPLETE_SPLIT(lpageop)) {
        elog(ERROR, "cannot insert to incompletely split page %u", BufferGetBlockNumber(buf));
    }

    itemsz = IndexTupleDSize(*itup);
    itemsz = MAXALIGN(itemsz);
    /*
     * Do we need to split the page to fit the item on it?
     *
     * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
     * so this comparison is correct even though we appear to be accounting
     * only for the item and not for its line pointer.
     */
    if (PageGetFreeSpace(page) < itemsz) {
        bool is_root = P_ISROOT(lpageop) > 0 ? true : false;
        bool is_only = P_LEFTMOST(lpageop) && P_RIGHTMOST(lpageop);
        bool newitemonleft = false;
        Buffer rbuf;
        /* Choose the split point */
        if (rel->rd_indexsplit == INDEXSPLIT_NO_INSERTPT) {
            /* Choose the split point */
            firstright = UBTreePCRFindsplitlocInsertpt(rel, buf, newitemoff, itemsz, &newitemonleft, itup);
        } else {
            firstright = UBTreePCRFindsplitloc(rel, buf, newitemoff, itemsz, &newitemonleft);
        }

        /* split the buffer into left and right halves */
        rbuf = UBTreePCRSplit(rel, buf, cbuf, firstright, newitemoff, itemsz, itup, newitemonleft, 
            itup_key, tdslot, urecptr, xlum, false);
        PredicateLockPageSplit(rel, BufferGetBlockNumber(buf), BufferGetBlockNumber(rbuf));

        /* ----------
         * By here,
         *
         *		+  our target page has been split;
         *		+  the original tuple has been inserted;
         *		+  we have write locks on both the old (left half)
         *		   and new (right half) buffers, after the split; and
         *		+  we know the key we want to insert into the parent
         *		   (it's the "high key" on the left child page).
         *
         * We're ready to do the parent insertion.  We need to hold onto the
         * locks for the child pages until we locate the parent, but we can
         * release them before doing the actual insertion (see Lehman and Yao
         * for the reasoning).
         * ----------
         */
        UBTreePCRInsertParent(rel, buf, rbuf, stack, is_root, is_only);

        /* ubtree will try to recycle one page after allocate one page */
        UBTreePCRTryRecycleEmptyPage(rel);
        return true;
    }
    Buffer metabuf = InvalidBuffer;
    Page metapg = NULL;
    BTMetaPageData *metad = NULL;
    OffsetNumber itup_off;
    BlockNumber itup_blkno;

    itup_off = newitemoff;
    itup_blkno = BufferGetBlockNumber(buf);

    /*
        * If we are doing this insert because we split a page that was the
        * only one on its tree level, but was not the root, it may have been
        * the "fast root".  We need to ensure that the fast root link points
        * at or above the current page.  We can safely acquire a lock on the
        * metapage here --- see comments for _bt_newroot().
        */
    if (split_only_page) {
        Assert(!P_ISLEAF(lpageop));
        metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
        metapg = BufferGetPage(metabuf);
        metad = BTPageGetMeta(metapg);
        if (metad->btm_fastlevel >= lpageop->btpo.level) {
            /* no update wanted */
            _bt_relbuf(rel, metabuf);
            metabuf = InvalidBuffer;
        }
    }

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    if (!UBTreePCRPageAddTuple(page, IndexTupleDSize(*itup), NULL, itup, newitemoff, false, tdslot)) {
        ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add new item to block %u in index \"%s\"",
                        itup_blkno, RelationGetRelationName(rel))));
    }

    if (P_ISLEAF(lpageop)) {
        TransactionId fxid = GetTopTransactionId();
        UBTreeTD td = UBTreePCRGetTD(page, tdslot);
        td->setInfo(fxid, urecptr);

        UndoRecord *undorec = (*t_thrd.ustore_cxt.urecvec)[0];
        undorec->SetOffset(newitemoff);
        undorec->SetBlkno(BufferGetBlockNumber(buf));

        UndoPersistence persistence = UndoPersistenceForRelation(rel);
        /* Insert the Undo record into the undo store */
        InsertPreparedUndo(t_thrd.ustore_cxt.urecvec);
        SetCurrentTransactionUndoRecPtr(urecptr, persistence);
        undo::PrepareUndoMeta(xlum, persistence, t_thrd.ustore_cxt.urecvec->LastRecord(),
            t_thrd.ustore_cxt.urecvec->LastRecordSize());
        undo::UpdateTransactionSlot(fxid, xlum, t_thrd.ustore_cxt.urecvec->FirstRecord(), persistence);
        lpageop->activeTupleCount++;
    }

    /* update active hint */
    
    MarkBufferDirty(buf);

    if (BufferIsValid(metabuf)) {
        metad->btm_fastroot = itup_blkno;
        rel->rd_rootcache = InvalidBuffer;
        metad->btm_fastlevel = lpageop->btpo.level;
        MarkBufferDirty(metabuf);
    }

    /* clear INCOMPLETE_SPLIT flag on child if inserting a downlink */
    if (BufferIsValid(cbuf)) {
        Page cpage = BufferGetPage(cbuf);
        UBTPCRPageOpaque cpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(cpage);
        Assert(P_INCOMPLETE_SPLIT(cpageop));
        cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
        MarkBufferDirty(cbuf);
    }

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
//         xl_btree_insert xlrec;
//         xl_btree_metadata_old xlmeta;
//         uint8 xlinfo;
//         XLogRecPtr recptr;
//         IndexTupleData trunctuple;

//         xlrec.offnum = itup_off;

//         XLogBeginInsert();
//         XLogRegisterData((char *)&xlrec, SizeOfBtreeInsert);

//         if (P_ISLEAF(lpageop)) {
//             xlinfo = XLOG_UBTREE_INSERT_LEAF;

//             /*
//              * Cache the block information if we just inserted into the
//              * rightmost leaf page of the index.
//             */
//             if (P_RIGHTMOST(lpageop)) {
//                 RelationSetTargetBlock(rel, BufferGetBlockNumber(buf));
//             }
//         } else {
//             /*
//              * Register the left child whose INCOMPLETE_SPLIT flag was
//              * cleared.
//              */
//             XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);
//             xlinfo = XLOG_UBTREE_INSERT_UPPER;
//         }

//         if (BufferIsValid(metabuf)) {
//             xlmeta.root = metad->btm_root;
//             xlmeta.level = metad->btm_level;
//             xlmeta.fastroot = metad->btm_fastroot;
//             xlmeta.fastlevel = metad->btm_fastlevel;

//             XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
//             XLogRegisterBufData(2, (char *)&xlmeta, sizeof(xl_btree_metadata_old));
//             xlinfo = XLOG_UBTREE_INSERT_META;
//         }

//         /* Read comments in _bt_pgaddtup */
//         XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
//         if (!P_ISLEAF(lpageop) && newitemoff == P_FIRSTDATAKEY(lpageop)) {
//             trunctuple = *itup;
//             trunctuple.t_info = sizeof(IndexTupleData);
//             XLogRegisterBufData(0, (char*)&trunctuple, sizeof(IndexTupleData));
//         } else {
//             ItemId iid = PageGetItemId(page, itup_off);
//             XLogRegisterBufData(0, (char*)PageGetItem(page, iid), itemsz);
//         }

//         recptr = XLogInsert(RM_UBTREE_ID, xlinfo);

//         if (BufferIsValid(metabuf)) {
//             PageSetLSN(metapg, recptr);
//         }
//         if (BufferIsValid(cbuf)) {
//             PageSetLSN(BufferGetPage(cbuf), recptr);
//         }
//         PageSetLSN(page, recptr);
    }

    if (P_ISLEAF(lpageop)) {
        undo::FinishUndoMeta(UndoPersistenceForRelation(rel));
    }
    END_CRIT_SECTION();
//     UBTreeVerify(rel, page, BufferGetBlockNumber(buf), itup_off, true);

    /* release buffers */
    if (BufferIsValid(metabuf)) {
        _bt_relbuf(rel, metabuf);
    }
    if (BufferIsValid(cbuf)) {
        _bt_relbuf(rel, cbuf);
    }
    _bt_relbuf(rel, buf);
    
    return false;
}


static void UBTreePCRDupInsertOnPage(Relation rel, Buffer buf, OffsetNumber offnum, int tdSlot,
    UndoRecPtr urecptr, undo::XlogUndoMeta *xlum)
{
    Page page = BufferGetPage(buf);;
    ItemId iid = UBTreePCRGetRowPtr(page, offnum);
    IndexTuple itup = UBTreePCRGetIndexTupleByItemId(page, iid);
    IndexTupleTrx trx = UBTreePCRGetIndexTupleTrx(itup);
    UBTPCRPageOpaque lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    START_CRIT_SECTION();

    UBTreePCRSetIndexTupleTrxSlot(trx, tdSlot);
    UBTreePCRClearIndexTupleTrxInvalid(trx);
    UBTreePCRClearIndexTupleDeleted(trx);

    iid->lp_flags = LP_NORMAL;

    TransactionId fxid = GetTopTransactionId();
    UBTreeTD td = UBTreePCRGetTD(page, tdSlot);
    td->setInfo(fxid, urecptr);

    UndoRecord *undorec = (*t_thrd.ustore_cxt.urecvec)[0];
    undorec->SetOffset(offnum);
    undorec->SetBlkno(BufferGetBlockNumber(buf));

    UndoPersistence persistence = UndoPersistenceForRelation(rel);
    /* Insert the Undo record into the undo store */
    InsertPreparedUndo(t_thrd.ustore_cxt.urecvec);
    SetCurrentTransactionUndoRecPtr(urecptr, persistence);
    undo::PrepareUndoMeta(xlum, persistence, t_thrd.ustore_cxt.urecvec->LastRecord(),
        t_thrd.ustore_cxt.urecvec->LastRecordSize());
    undo::UpdateTransactionSlot(fxid, xlum, t_thrd.ustore_cxt.urecvec->FirstRecord(), persistence);
    lpageop->activeTupleCount++;

    /* update active hint */
    MarkBufferDirty(buf);
    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
// //         xl_btree_insert xlrec;
// //         xl_btree_metadata_old xlmeta;
// //         uint8 xlinfo;
// //         XLogRecPtr recptr;
// //         IndexTupleData trunctuple;

// //         xlrec.offnum = itup_off;

// //         XLogBeginInsert();
// //         XLogRegisterData((char *)&xlrec, SizeOfBtreeInsert);

// //         if (P_ISLEAF(lpageop)) {
// //             xlinfo = XLOG_UBTREE_INSERT_LEAF;

// //             /*
// //              * Cache the block information if we just inserted into the
// //              * rightmost leaf page of the index.
// //             */
// //             if (P_RIGHTMOST(lpageop)) {
// //                 RelationSetTargetBlock(rel, BufferGetBlockNumber(buf));
// //             }
// //         } else {
// //             /*
// //              * Register the left child whose INCOMPLETE_SPLIT flag was
// //              * cleared.
// //              */
// //             XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);
// //             xlinfo = XLOG_UBTREE_INSERT_UPPER;
// //         }

// //         if (BufferIsValid(metabuf)) {
// //             xlmeta.root = metad->btm_root;
// //             xlmeta.level = metad->btm_level;
// //             xlmeta.fastroot = metad->btm_fastroot;
// //             xlmeta.fastlevel = metad->btm_fastlevel;

// //             XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
// //             XLogRegisterBufData(2, (char *)&xlmeta, sizeof(xl_btree_metadata_old));
// //             xlinfo = XLOG_UBTREE_INSERT_META;
// //         }

// //         /* Read comments in _bt_pgaddtup */
// //         XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
// //         if (!P_ISLEAF(lpageop) && newitemoff == P_FIRSTDATAKEY(lpageop)) {
// //             trunctuple = *itup;
// //             trunctuple.t_info = sizeof(IndexTupleData);
// //             XLogRegisterBufData(0, (char*)&trunctuple, sizeof(IndexTupleData));
// //         } else {
// //             ItemId iid = PageGetItemId(page, itup_off);
// //             XLogRegisterBufData(0, (char*)PageGetItem(page, iid), itemsz);
// //         }

// //         recptr = XLogInsert(RM_UBTREE_ID, xlinfo);

// //         if (BufferIsValid(metabuf)) {
// //             PageSetLSN(metapg, recptr);
// //         }
// //         if (BufferIsValid(cbuf)) {
// //             PageSetLSN(BufferGetPage(cbuf), recptr);
// //         }
// //         PageSetLSN(page, recptr);
    }
    undo::FinishUndoMeta(UndoPersistenceForRelation(rel));
    END_CRIT_SECTION();
// //     UBTreeVerify(rel, page, BufferGetBlockNumber(buf), itup_off, true);

    _bt_relbuf(rel, buf);
}


/*
 *	UBTreePCRGetStackBuf() -- Walk back up the tree one step, and find the item
 *						 we last looked at in the parent.
 *
 *		This is possible because we save the downlink from the parent item,
 *		which is enough to uniquely identify it.  Insertions into the parent
 *		level could cause the item to move right; deletions could cause it
 *		to move left, but not left of the page we previously found it in.
 *
 *		Adjusts bts_blkno & bts_offset if changed.
 *
 *		Returns InvalidBuffer if item not found (should not happen).
 */
Buffer UBTreePCRGetStackBuf(Relation rel, BTStack stack)
{
    BlockNumber blkno;
    OffsetNumber start;

    blkno = stack->bts_blkno;
    start = stack->bts_offset;
    t_thrd.storage_cxt.is_btree_split = true;
    for (;;) {
        Buffer buf;
        Page page;
        UBTPCRPageOpaque opaque;

        buf = _bt_getbuf(rel, blkno, BT_WRITE);
        page = BufferGetPage(buf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (P_INCOMPLETE_SPLIT(opaque)) {
            UBTreePCRFinishSplit(rel, buf, stack->bts_parent);
            continue;
        }

        if (!P_IGNORE(opaque)) {
            OffsetNumber offnum, minoff, maxoff;
            ItemId itemid;
            IndexTuple item;

            minoff = P_FIRSTDATAKEY(opaque);
            maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

            /*
             * start = InvalidOffsetNumber means "search the whole page". We
             * need this test anyway due to possibility that page has a high
             * key now when it didn't before.
             */
            if (start < minoff) {
                start = minoff;
            }

            /*
             * Need this check too, to guard against possibility that page
             * split since we visited it originally.
             */
            if (start > maxoff) {
                start = OffsetNumberNext(maxoff);
            }

            /*
             * These loops will check every item on the page --- but in an
             * order that's attuned to the probability of where it actually
             * is.	Scan to the right first, then to the left.
             */
            for (offnum = start; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
                itemid = UBTreePCRGetRowPtr(page, offnum);
                item = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
                bool getRightItem = false;
                getRightItem = BTreeInnerTupleGetDownLink(item) == stack->bts_btentry;
                if (getRightItem) {
                    /* Return accurate pointer to where link is now */
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    t_thrd.storage_cxt.is_btree_split = false;
                    return buf;
                }
            }

            for (offnum = OffsetNumberPrev(start); offnum >= minoff; offnum = OffsetNumberPrev(offnum)) {
                itemid = UBTreePCRGetRowPtr(page, offnum);
                item = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
                bool getRightItem = false;
                getRightItem = BTreeInnerTupleGetDownLink(item) == stack->bts_btentry;
                if (getRightItem) {
                    /* Return accurate pointer to where link is now */
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    t_thrd.storage_cxt.is_btree_split = false;
                    return buf;
                }
            }
        }

        /*
         * The item we're looking for moved right at least one page.
         */
        if (P_RIGHTMOST(opaque)) {
            _bt_relbuf(rel, buf);
            t_thrd.storage_cxt.is_btree_split = false;
            return InvalidBuffer;
        }
        blkno = opaque->btpo_next;
        start = InvalidOffsetNumber;
        _bt_relbuf(rel, buf);
    }
}

void UBTreeResetWaitTimeForTDSlot()
{
    t_thrd.ustore_cxt.tdSlotWaitActive = false;
    t_thrd.ustore_cxt.tdSlotWaitFinishTime = 0;
}

/*
 * UBTreePCRInsertParent() -- Insert downlink into parent after a page split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on per the L&Y algorithm.  We release the
 * write locks once we have write lock on the parent page. (Any sooner,
 * and it'd be possible for some other process to try to split or delete
 * one of these pages, and get confused because it cannot find the downlink.)
 *
 * stack - stack showing how we got here.  May be NULL in cases that don't
 *         have to be efficient (concurrent ROOT split, WAL recovery)
 * is_root - we split the true root
 * is_only - we split a page alone on its level (might have been fast root)
 */
void UBTreePCRInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only)
{
    /*
     * Here we have to do something Lehman and Yao don't talk about: deal with
     * a root split and construction of a new root.  If our stack is empty
     * then we have just split a node on what had been the root level when we
     * descended the tree. If it was still the root then we perform a
     * new-root construction.  If it *wasn't* the root anymore, search to find
     * the next higher level that someone constructed meanwhile, and find the
     * right place to insert as for the normal case.
     *
     * If we have to search for the parent level, we do so by re-descending
     * from the root.  This is not super-efficient, but it's rare enough not
     * to matter.  (This path is also taken when called from WAL recovery ---
     * we have no stack in that case.)
     */
    if (is_root) {
        Buffer rootbuf;

        Assert(stack == NULL);
        Assert(is_only);
        /* create a new root node and update the metapage */
        rootbuf = UBTreePCRNewRoot(rel, buf, rbuf);
        /* release the split buffers */
        _bt_relbuf(rel, rootbuf);
        _bt_relbuf(rel, rbuf);
        _bt_relbuf(rel, buf);
    } else {
        BlockNumber bknum = BufferGetBlockNumber(buf);
        BlockNumber rbknum = BufferGetBlockNumber(rbuf);
        Page page = BufferGetPage(buf);
        IndexTuple new_item;
        BTStackData fakestack;
        IndexTuple ritem;
        Buffer pbuf;

        if (stack == NULL) {
            UBTPCRPageOpaque lpageop;

            if (!t_thrd.xlog_cxt.InRecovery) {
                ereport(DEBUG2, (errmsg("concurrent ROOT page split")));
            }
            lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            /* Find the leftmost page at the next level up */
            pbuf = UBTreePCRGetEndPoint(rel, lpageop->btpo.level + 1, false);
            /* Set up a phony stack entry pointing there */
            stack = &fakestack;
            stack->bts_blkno = BufferGetBlockNumber(pbuf);
            stack->bts_offset = InvalidOffsetNumber;
            stack->bts_btentry = InvalidBlockNumber;
            stack->bts_parent = NULL;
            _bt_relbuf(rel, pbuf);
        }
        
        /* get high key from left, a strict lower bound for new right page */
        ritem = (IndexTuple)UBTreePCRGetIndexTuple(page,  P_HIKEY);

        /* form an index tuple that points at the new right page
         * assure that memory is properly allocated, prevent from missing log of insert parent */
        START_CRIT_SECTION();
        new_item = CopyIndexTuple(ritem);
        UBTreeTupleSetDownLink(new_item, rbknum);
        END_CRIT_SECTION();

        /*
         * Find the parent buffer and get the parent page.
         *
         * Oops - if we were moved right then we need to change stack item! We
         * want to find parent pointing to where we are, right ? - vadim
         * 05/27/97
         */
        stack->bts_btentry = bknum;
        pbuf = UBTreePCRGetStackBuf(rel, stack);

        /* Now we can unlock the right child. The left child will be unlocked
         * by _bt_insertonpg()
         */
        _bt_relbuf(rel, rbuf);

        /* Check for error only after writing children */
        if (pbuf == InvalidBuffer) {
            XLogWaitFlush(t_thrd.xlog_cxt.LogwrtResult->Write);
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to re-find parent key in index \"%s\" for split pages %u/%u",
                           RelationGetRelationName(rel), bknum, rbknum)));
        }
        /* Recursively update the parent */
        UBTreePCRInsertOnPage(rel, NULL, pbuf, buf, stack->bts_parent, new_item, stack->bts_offset + 1, is_only,
            UBTreeInvalidTDSlotId, INVALID_UNDO_REC_PTR, NULL);

        /* be tidy */
        pfree(new_item);
    }
}


/*
 * UBTreePCRFinishSplit() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _bt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode. On exit, it is unlocked
 * and unpinned.
 */
void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack)
{
    Page lpage = BufferGetPage(lbuf);
    UBTPCRPageOpaque lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(lpage);
    Buffer rbuf;
    Page rpage;
    UBTPCRPageOpaque rpageop;
    bool wasRoot = false;

    Assert(P_INCOMPLETE_SPLIT(lpageop));

    /* Lock right sibling, the one missing the downlink */
    rbuf = _bt_getbuf(rel, lpageop->btpo_next, BT_WRITE);
    rpage = BufferGetPage(rbuf);
    rpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(rpage);

    /* Could this be a root split? */
    if (!stack) {
        Buffer metabuf;
        Page metapg;
        BTMetaPageData *metad;

        /* acquire lock on the metapage */
        metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
        metapg = BufferGetPage(metabuf);
        metad = BTPageGetMeta(metapg);

        wasRoot = (metad->btm_root == BufferGetBlockNumber(lbuf));

        _bt_relbuf(rel, metabuf);
    } else {
        wasRoot = false;
    }

    /* Was this the only page on the level before split? */
    bool wasOnly = (P_LEFTMOST(lpageop) && P_RIGHTMOST(rpageop));

    elog(DEBUG1, "finishing incomplete split of %u/%u", BufferGetBlockNumber(lbuf), BufferGetBlockNumber(rbuf));

    UBTreePCRInsertParent(rel, lbuf, rbuf, stack, wasRoot, wasOnly);
}


/*
 *	UBTreePCRFindInsertLoc() -- Finds an insert location for a tuple
 *
 *	    On entry, *buf and *offsetptr point to the first legal position
 *		where the new tuple could be inserted.	The caller should hold an
 *		exclusive lock on *buf.  *offsetptr can also be set to
 *		InvalidOffsetNumber, in which case the function will search for the
 *		right location within the page if needed.
 *
 *	    If 'checkingunique' is true, the buffer on entry is the first page
 *		that contains duplicates of the new key.  If there are duplicates on
 *		multiple pages, the correct insertion position might be some page to
 *		the right, rather than the first page.  In that case, this function
 *		moves right to the correct target page.
 *
 *		If the new key is equal to one or more existing keys, we can
 *		legitimately place it anywhere in the series of equal keys --- in fact,
 *		if the new key is equal to the page's "high key" we can place it on
 *		the next page.	If it is equal to the high key, and there's not room
 *		to insert the new tuple on the current page without splitting, then
 *		we can move right hoping to find more free space and avoid a split.
 *		(We should not move right indefinitely, however, since that leads to
 *		O(N^2) insertion behavior in the presence of many equal keys.)
 *		Once we have chosen the page to put the key on, we'll insert it before
 *		any existing equal keys because of the way _bt_binsrch() works.
 *
 *		(In a !heapkeyspace index, there can be multiple pages with the same
 *		high key, where the new tuple could legitimately be placed on.  In
 *		that case, the caller passes the first page containing duplicates,
 *		just like when checkinunique=true.  If that page doesn't have enough
 *		room for the new tuple, this function moves right, trying to find a
 *		legal page that does.)
 *
 *		On exit, *buf and *offsetptr point to the chosen insert location. If
 *		_bt_findinsertloc decides to move right, the lock and pin on the
 *		original page will be released and the new page returned to the caller
 *		is exclusively locked instead.
 *
 *		If there is not enough room on the page for the new tuple, we try to
 *		make room by removing any LP_DEAD tuples. (Try pruning page in UStore's
 *		multi-version index)
 *
 *		newtup is the new tuple we're inserting, and scankey is an insertion
 *		type scan key for it.
 */
static OffsetNumber UBTreePCRFindInsertLoc(Relation rel, Buffer *bufptr, OffsetNumber firstlegaloff, BTScanInsert itup_key,
    IndexTuple newtup, bool checkingunique, BTStack stack, Relation heapRel)
{
    Buffer buf = *bufptr;
    Page page = BufferGetPage(buf);
    Size itemsz;
    UBTPCRPageOpaque lpageop;
    bool movedright = false;
    bool pruned = false;

    lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    itemsz = IndexTupleSize(newtup);
    itemsz = MAXALIGN(itemsz); /* be safe, PageAddItem will do this but we
                                * need to be consistent */
    /* Check 1/3 of a page restriction */
    if (unlikely(itemsz > UBTreePCRMaxItemSize(page))) {
        UBTreeCheckThirdPage<UBTPCRPageOpaque>(rel, heapRel, itup_key->heapkeyspace, page, newtup);
    }

    Assert(P_ISLEAF(lpageop) && !P_INCOMPLETE_SPLIT(lpageop));
    Assert(!itup_key->heapkeyspace || itup_key->scantid != NULL);
    Assert(itup_key->heapkeyspace || itup_key->scantid == NULL);

    /* heapkeyspace always set in ubtree */
    Assert(itup_key->heapkeyspace);
    /*
     * If we're inserting into a unique index, we may have to walk right
     * through leaf pages to find the one leaf page that we must insert on
     * to.
     *
     * This is needed for checkingunique callers because a scantid was not
     * used when we called _bt_search().  scantid can only be set after
     * _bt_check_unique() has checked for duplicates.  The buffer
     * initially stored in insertstate->buf has the page where the first
     * duplicate key might be found, which isn't always the page that new
     * tuple belongs on.  The heap TID attribute for new tuple (scantid)
     * could force us to insert on a sibling page, though that should be
     * very rare in practice.
     */
    if (checkingunique) {
        while (!P_RIGHTMOST(lpageop) && UBTreePCRCompare(rel, itup_key, page, P_HIKEY) > 0) {
            /* The new tuple belongs on the next page, try to step right */
            Buffer rbuf = InvalidBuffer;
            BlockNumber rblkno = lpageop->btpo_next;

            for (;;) {
                rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
                page = BufferGetPage(rbuf);
                lpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
                /*
                 * If this page was incompletely split, finish the split now.
                 * We do this while holding a lock on the left sibling, which
                 * is not good because finishing the split could be a fairly
                 * lengthy operation.  But this should happen very seldom.
                 */
                if (P_INCOMPLETE_SPLIT(lpageop)) {
                    UBTreePCRFinishSplit(rel, rbuf, stack);
                    rbuf = InvalidBuffer;
                    continue;
                }
                if (!P_IGNORE(lpageop))
                    break;
                if (P_RIGHTMOST(lpageop))
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\" at blkno %u",
                                   RelationGetRelationName(rel), rblkno)));

                rblkno = lpageop->btpo_next;
            }
            _bt_relbuf(rel, buf);
            buf = rbuf;
            movedright = true;
            pruned = false;
        }
    }

    /*
     * If the target page is full, see if we can obtain enough space by:
     *      (a) pruning this page if we have multi-version info.
     *      (b) erasing LP_DEAD items.
     */
    if (PageGetFreeSpace(page) < itemsz) {
        if (P_ISLEAF(lpageop)) {
            if (UBTreePCRPagePruneOpt(rel, buf, false, NULL)) {
                /* just like vacuum, we mark pruned as true to find the newitemoff again */
                pruned = true;
            }
        }
    }

    /*
     * Now we are on the right page, so find the insert position. If we moved
     * right at all, we know we should insert at the start of the page. If we
     * didn't move right, we can use the firstlegaloff hint if the caller
     * supplied one, unless we pruned the page which might have moved tuples
     * around making the hint invalid.
     *
     * After we take TID as tie-breaker, tuples with the same key value need
     * to be sorted by TID. Since firstlegaloff does not consider TID, this
     * hint may not be accurate. We need to examine with _bt_compare() again,
     * because TID is already included in itup_key now (after checking unique).
     */

    *bufptr = buf;

    if (movedright) {
        /*
         * moving right means we may insert in the front of the page, but still
         * need to check carefully when itup_key->heapkeyspace.
         */
        firstlegaloff = P_FIRSTDATAKEY(lpageop);
    }

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (firstlegaloff != InvalidOffsetNumber && !pruned &&
        (firstlegaloff > maxoff || UBTreePCRCompare(rel, itup_key, page, firstlegaloff) <= 0)) {
        return firstlegaloff;
    }

    /* otherwise, we need to call _bt_binsrch() again with TID to find the insertion location */
    return UBTreePCRBinarySearch(rel, itup_key, BufferGetPage(buf));
}

Buffer UBTPCRGetNextPage(Relation rel, Buffer buf, BTStack stack)
{
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    while (true) {
        buf = _bt_relandgetbuf(rel, buf, opaque->btpo_next, BT_WRITE);
        Assert(buf != InvalidBuffer);
        page = BufferGetPage(buf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (P_IGNORE(opaque)) {
            Assert(!P_RIGHTMOST(opaque));
            continue;
        }
        if (P_INCOMPLETE_SPLIT(opaque)) {
            BlockNumber blkno = BufferGetBlockNumber(buf);
            UBTreePCRFinishSplit(rel, buf, stack);
            buf = _bt_getbuf(rel, blkno, BT_WRITE);
            page = BufferGetPage(buf);
        }
        break;
    }
    return buf;
}

bool UBTPCRIsNormalInsert(Relation rel, Buffer& pbuf, OffsetNumber& offnum, IndexTuple itup,
    BTScanInsert itupKye, BTStack stack)
{
    Buffer buf = pbuf;
    bool isIncluding = UBTreeHasIncluding(rel);
    Oid partOid = InvalidOid;
    AttrNumber attr = IndexRelationGetNumberOfAttributes(rel);
    TupleDesc desc = RelationGetDescr(rel);
    bool isNull = false;
    if (RelationIsGlobalIndex(rel)) {
        partOid = DatumGetInt32(index_getattr(itup, attr, desc, &isNull));
    }
loop:
    Page page = BufferGetPage(buf);
    OffsetNumber maxOff = UBTreePCRPageGetMaxOffsetNumber(page);
    OffsetNumber endOff = maxOff;
    Size size = IndexTupleSize(itup) - sizeof(IndexTupleTrxData);
    if (isIncluding) {
        itupKye->nextkey = true;
        endOff = UBTreePCRBinarySearch(rel, itupKye, page) - 1;
        itupKye->nextkey = false;
        while (offnum <= endOff) {
            IndexTuple curItup = UBTreePCRGetIndexTuple(page, offnum);
            if (size == IndexTupleSize(curItup) && memcmp(curItup, itup, size)) {
                return false;
            }
            offnum ++;
        }
    } else {
        while (offnum <= endOff) {
            IndexTuple curItup = UBTreePCRGetIndexTuple(page, offnum);
            if (!ItemPointerEquals(&curItup->t_tid, &itup->t_tid) || !UBTreePCRIsKeyEqual(rel, curItup, itupKye)) {
                break;
            }
            if (RelationIsGlobalIndex(rel)) {
                if (DatumGetInt32(index_getattr(curItup, attr, desc, &isNull)) == partOid) {
                    return false;
                }
            } else {
                return false;
            }
            offnum ++;
        }
    }
    
    if ((!isIncluding && RelationIsGlobalIndex(rel) && offnum > maxOff) || 
        (isIncluding && offnum > maxOff)) {
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (P_RIGHTMOST(opaque) || UBTreePCRCompare(rel, itupKye, page, P_HIKEY) != 0) {
            return true;
        }
        buf = UBTPCRGetNextPage(rel, buf, stack);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        offnum = P_FIRSTDATAKEY(opaque);
        pbuf = buf;
        goto loop;
    }
    return true;
}

/*
 *	UBTreePCRCheckUnique() -- Check for violation of unique index constraint
 *
 * offset points to the first possible item that could conflict. It can
 * also point to end-of-page, which means that the first tuple to check
 * is the first tuple on the next page.
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.	If an actual
 * conflict is detected, no return --- just ereport().
 *
 * However, if checkUnique == UNIQUE_CHECK_PARTIAL, we always return
 * InvalidTransactionId because we don't want to wait.  In this case we
 * set *is_unique to false if there is a potential conflict, and the
 * core code must redo the uniqueness check later.
 */
static TransactionId UBTreePCRCheckUnique(Relation rel, IndexTuple itup, Relation heapRel, Buffer buf, OffsetNumber offset,
    BTScanInsert itup_key, IndexUniqueCheck checkUnique, bool* is_unique, GPIScanDesc gpiScan)
{
    SnapshotData SnapshotDirty;
    OffsetNumber maxoff;
    Page page;
    UBTPCRPageOpaque opaque;
    Buffer nbuf = InvalidBuffer;
    bool found = false;
    Relation tarRel = heapRel;

    WHITEBOX_TEST_STUB("UBTreePCRCheckUnique", WhiteboxDefaultErrorEmit);

    /* Assume unique until we find a duplicate */
    *is_unique = true;

    InitDirtySnapshot(SnapshotDirty);

    page = BufferGetPage(buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    /*
     * Scan over all equal tuples, looking for live conflicts.
     */
    Assert(!itup_key->anynullkeys);
    Assert(itup_key->scantid == NULL);
    for (;;) {
        ItemId curitemid;
        IndexTuple curitup;
        BlockNumber nblkno;

        /*
         * make sure the offset points to an actual item before trying to
         * examine it...
         */
        if (offset <= maxoff) {
            curitemid = UBTreePCRGetRowPtr(page, offset);
            /*
             * We can skip items that are marked killed.
             *
             * Formerly, we applied _bt_isequal() before checking the kill
             * flag, so as to fall out of the item loop as soon as possible.
             * However, in the presence of heavy update activity an index may
             * contain many killed items with the same key; running
             * _bt_isequal() on each killed item gets expensive. Furthermore
             * it is likely that the non-killed version of each key appears
             * first, so that we didn't actually get to exit any sooner
             * anyway. So now we just advance over killed items as quickly as
             * we can. We only apply _bt_isequal() when we get to a non-killed
             * item or the end of the page.
             */
            if (!ItemIdIsDead(curitemid)) {
                ItemPointerData htid;

                /*
                 * _bt_compare returns 0 for (1,NULL) and (1,NULL) - this's
                 * how we handling NULLs - and so we must not use _bt_compare
                 * in real comparison, but only for ordering/finding items on
                 * pages. - vadim 03/24/97
                 */
                if (!UBTreeIsEqual<UBTPCRPageOpaque>(rel, page, offset, itup_key->keysz, itup_key->scankeys)) {
                    break; /* we're past all the equal tuples */
                }

                /* okay, we gotta fetch the heap tuple ... */
                curitup = (IndexTuple) UBTreePCRGetIndexTuple(page, offset);
                htid = curitup->t_tid;
                Oid curPartOid = InvalidOid;
                Datum datum;
                bool isNull;
                if (RelationIsGlobalIndex(rel)) {
                    datum = index_getattr(curitup, IndexRelationGetNumberOfAttributes(rel),
                                          RelationGetDescr(rel), &isNull);
                    curPartOid = DatumGetUInt32(datum);
                    Assert(isNull == false);
                    if (curPartOid != gpiScan->currPartOid) {
                        GPISetCurrPartOid(gpiScan, curPartOid);
                        if (!GPIGetNextPartRelation(gpiScan, CurrentMemoryContext, AccessShareLock)) {
                            if (CheckPartitionIsInvisible(gpiScan)) {
                                ItemIdMarkDead(curitemid);
                            }
                            opaque->btpo_flags |= BTP_HAS_GARBAGE;
                            if (nbuf != InvalidBuffer) {
                                MarkBufferDirtyHint(nbuf, true);
                            } else {
                                MarkBufferDirtyHint(buf, true);
                            }
                            goto next;
                        } else {
                            tarRel = gpiScan->fakePartRelation;
                        }
                    }
                }

                /*
                 * If we are doing an index recheck (UNIQUE_CHECK_EXISTING mode), we expect
                 * to find tuple already in ubtree. Once the index tuple matched, it will be
                 * marked found. Traverse all tuples, if no matching tuple is found, report
                 * an error. For GPI, part oid should be the same as heapRel oid.
                 */
                if (checkUnique == UNIQUE_CHECK_EXISTING && ItemPointerCompare(&htid, &itup->t_tid) == 0
                    && !RelationIsGlobalIndex(rel)) {
                    found = true;
                } else if (checkUnique == UNIQUE_CHECK_EXISTING && ItemPointerCompare(&htid, &itup->t_tid) == 0
                    && curPartOid == heapRel->rd_id && RelationIsGlobalIndex(rel)) {
                    found = true;
                } else {
                    for (;;) {
                        ItemId iid = curitemid;
                        IndexTupleTrx trx = UBTreePCRGetIndexTupleTrx(curitup);
                        /* here we guarantee the same semantics as UNIQUE_CHECK_PARTIAL */
                        if (checkUnique == UNIQUE_CHECK_PARTIAL) {
                            if (!IsUBTreePCRItemDeleted(trx)) {
                                if (nbuf != InvalidBuffer) {
                                    _bt_relbuf(rel, nbuf);
                                }
                                *is_unique = false;
                                return InvalidTransactionId;
                            }
                        }
                        if (UBTreeTDSlotIsNormal(trx->tdSlot)) {
                            UBTreeTD td = UBTreePCRGetTD(page, trx->tdSlot);
                            TransactionId xid = td->xactid;
                            /* xid on tuple is not current and valid, return and wait */
                            if (!TransactionIdIsCurrentTransactionId(xid)) {
                                if (!IsUBTreePCRTDReused(trx) && TransactionIdIsInProgress(xid)) {
                                    if (nbuf != InvalidBuffer) {
                                        _bt_relbuf(rel, nbuf);
                                    }
                                    return xid;
                                }
                            }
                            
                            if (!IsUBTreePCRTDReused(trx) && TransactionIdIsValid(xid) && (!UBTreePCRTDIsCommited(td) 
                                || !UHeapTransactionIdDidCommit(xid))) {
                                if (nbuf != InvalidBuffer) {
                                    // _bt_relbuf(rel, nbuf);
                                    LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
                                    LockBuffer(nbuf, BT_WRITE);
                                    ExecuteUndoActionsForUBTreePage(rel, nbuf, trx->tdSlot);
                                    LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
                                    LockBuffer(nbuf, BT_READ);
                                } else {
                                    ExecuteUndoActionsForUBTreePage(rel, nbuf, trx->tdSlot);
                                }
                            }
                        }

                        // TODO: 要理解一下这里的状态
                        if (!IsUBTreePCRItemDeleted(iid)) {
                            Datum values[INDEX_MAX_KEYS];
                            bool isnull[INDEX_MAX_KEYS];
                            char *key_desc = NULL;

                            index_deform_tuple(itup, RelationGetDescr(rel), values, isnull);

                            key_desc = BuildIndexValueDescription(rel, values, isnull);

                            ereport(ERROR, (errcode(ERRCODE_UNIQUE_VIOLATION),
                                        errmsg("duplicate key value violates unique constraint \"%s\"",
                                               RelationGetRelationName(rel)),
                                        key_desc ? errdetail("Key %s already exists.", key_desc) : 0));
                        }
                        break;
                    }
                }
            }
        }

next:
        /*
         * Advance to next tuple to continue checking.
         */
        if (offset < maxoff) {
            offset = OffsetNumberNext(offset);
        } else {
            /* If scankey == hikey we gotta check the next page too */
            if (P_RIGHTMOST(opaque)) {
                break;
            }
            int highkeycmp = UBTreePCRCompare(rel, itup_key, page, P_HIKEY);
            Assert(highkeycmp <= 0);
            if (highkeycmp != 0) {
                break;
            }
            /* Advance to next non-dead page --- there must be one */
            for (;;) {
                nblkno = opaque->btpo_next;
                nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
                page = BufferGetPage(nbuf);
                opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
                if (!P_IGNORE(opaque)) {
                    break;
                }
                if (P_RIGHTMOST(opaque)) {
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\" at blkno %u",
                                   RelationGetRelationName(rel), nblkno)));
                }
            }
            maxoff = UBTreePCRPageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }

    /*
     * If we are doing a recheck then we should have found the tuple we are
     * checking.  Otherwise there's something very wrong --- probably, the
     * index is on a non-immutable expression.
     */
    if (checkUnique == UNIQUE_CHECK_EXISTING && !found)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to re-find tuple within index \"%s\"", RelationGetRelationName(rel)),
                errhint("This may be because of a non-immutable index expression.")));

    if (nbuf != InvalidBuffer)
        _bt_relbuf(rel, nbuf);

    return InvalidTransactionId;
}

/*
*   UBTreePCRCopyTDSlot
*       copy td slot for split page.
*/
void UBTreePCRCopyTDSlot(Page origin, Page target)
{  
    UBTPCRPageOpaque oOpaque = (UBTPCRPageOpaque)PageGetSpecialPointer(origin);
    UBTPCRPageOpaque tOpaque = (UBTPCRPageOpaque)PageGetSpecialPointer(target);

    tOpaque->td_count = oOpaque->td_count;
    Size size = SizeOfUBTreeTDData(origin);
    errno_t rc = memcpy_s((char*)target + SizeOfPageHeaderData, size, (char*)target + SizeOfPageHeaderData, size);
    securec_check(rc, "", "");
    ((PageHeader)target)->pd_lower += size;
}

/*
 *	UBTreePCRSplit() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		firstright is the item index of the first item to be moved to the
 *		new right page.  newitemoff etc. tell us about the new item that
 *		must be inserted along with the data from the old page.
 *
 *		itup_key is used for suffix truncation on leaf pages (internal
 *		page callers pass NULL).  When splitting a non-leaf page, 'cbuf'
 *		is the left-sibling of the page we're inserting the downlink for.
 *		This function will clear the INCOMPLETE_SPLIT flag on it, and
 *		release the buffer.
 *
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
static Buffer UBTreePCRSplit(Relation rel, Buffer buf, Buffer cbuf, OffsetNumber firstrightoff, OffsetNumber newitemoff,
    Size newitemsz, IndexTuple newitem, bool newitemonleft, BTScanInsert itup_key,uint8 tdslot, UndoRecPtr urec,
    undo::XlogUndoMeta *meta, bool noinsert)
{
    Buffer rbuf;
    Page origpage;
    Page leftpage, rightpage;
    BlockNumber origpagenumber, rightpagenumber;
    UBTPCRPageOpaque ropaque, lopaque, oopaque;
    Buffer sbuf = InvalidBuffer;
    Page spage = NULL;
    UBTPCRPageOpaque sopaque = NULL;
    Size itemsz;
    ItemId itemid;
    IndexTuple item;
    OffsetNumber leftoff = InvalidOffsetNumber;
    OffsetNumber rightoff = InvalidOffsetNumber;
    OffsetNumber newitemleftoff = InvalidOffsetNumber;
    OffsetNumber maxoff;
    OffsetNumber i;
    bool isroot = false;
    bool isleaf = false;
    errno_t rc;
    IndexTuple firstright, lefthighkey;
    Buffer actualInsertBuf = InvalidBuffer;
    OffsetNumber actualInsertOff = InvalidOffsetNumber;

    WHITEBOX_TEST_STUB("UBTreePCRSplit", WhiteboxDefaultErrorEmit);

    /*
     * Acquire a new page to split into.
     *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
     *            before we release the Exclusive lock.
     */
    UBTRecycleQueueAddress addr;
    rbuf = UBTreePCRGetNewPage(rel, &addr);

    /*
     * origpage is the original page to be split.  leftpage is a temporary
     * buffer that receives the left-sibling data, which will be copied back
     * into origpage on success.  rightpage is the new page that receives the
     * right-sibling data.	If we fail before reaching the critical section,
     * origpage hasn't been modified and leftpage is only workspace. In
     * principle we shouldn't need to worry about rightpage either, because it
     * hasn't been linked into the btree page structure; but to avoid leaving
     * possibly-confusing junk behind, we are careful to rewrite rightpage as
     * zeroes before throwing any error.
     */
    origpage = BufferGetPage(buf);
    leftpage = PageGetTempPage(origpage);
    rightpage = BufferGetPage(rbuf);

    origpagenumber = BufferGetBlockNumber(buf);
    rightpagenumber = BufferGetBlockNumber(rbuf);

    UBTreePCRPageInit(leftpage, BufferGetPageSize(buf));
    /* rightpage was already initialized by _bt_getbuf
     * Copy the original page's LSN into leftpage, which will become the
     * updated version of the page.  We need this because XLogInsert will
     * examine the LSN and possibly dump it in a page image.
     */
    PageSetLSN(leftpage, PageGetLSN(origpage));

    /* init btree private data */
    oopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(origpage);
    lopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(leftpage);
    ropaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rightpage);

    isroot = P_ISROOT(oopaque) > 0 ? true : false;
    isleaf = P_ISLEAF(oopaque) > 0 ? true : false;
    if (noinsert) {
        isleaf = true;
    }

    /* if we're splitting this page, it won't be the root when we're done */
    /* also, clear the SPLIT_END and HAS_GARBAGE flags in both pages */
    lopaque->btpo_flags = oopaque->btpo_flags;
    lopaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    ropaque->btpo_flags = lopaque->btpo_flags;
    /* set flag in left page indicating that the right page has no downlink */
    lopaque->btpo_flags |= BTP_INCOMPLETE_SPLIT;
    lopaque->btpo_prev = oopaque->btpo_prev;
    lopaque->btpo_next = rightpagenumber;
    ropaque->btpo_prev = origpagenumber;
    ropaque->btpo_next = oopaque->btpo_next;
    lopaque->btpo.level = ropaque->btpo.level = oopaque->btpo.level;
    /* Since we already have write-lock on both pages, ok to read cycleid */
    lopaque->btpo_cycleid = oopaque->btpo_cycleid;
    ropaque->btpo_cycleid = oopaque->btpo_cycleid;
    /* copy other fields */
    lopaque->last_delete_xid = oopaque->last_delete_xid;
    ropaque->last_delete_xid = oopaque->last_delete_xid;
    /* reset the active hint, update later */
    lopaque->activeTupleCount = 0;
    ropaque->activeTupleCount = 0;
    lopaque->last_commit_xid = oopaque->last_commit_xid;
    ropaque->last_commit_xid = oopaque->last_commit_xid;
    lopaque->flags = oopaque->flags;
    ropaque->flags = oopaque->flags;

    if (isleaf) {
        UBTreePCRCopyTDSlot(origpage, rightpage);
        UBTreePCRCopyTDSlot(origpage, leftpage);
    }

    /*
     * If the page we're splitting is not the rightmost page at its level in
     * the tree, then the first entry on the page is the high key for the
     * page.  We need to copy that to the right half.  Otherwise (meaning the
     * rightmost page case), all the items on the right half will be user
     * data.
     */
    rightoff = P_HIKEY;

    if (!P_RIGHTMOST(oopaque)) {
        itemid = UBTreePCRGetRowPtr(origpage, P_HIKEY);
        itemsz = ItemIdGetLength(itemid);
        item = (IndexTuple)UBTreePCRGetIndexTupleByItemId(origpage, itemid);
        Assert(UBTreeTupleGetNAtts(item, rel) > 0);
        Assert(UBTreeTupleGetNAtts(item, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
        if (UBTPCRPageAddItem(rightpage, (Item)item, itemsz, rightoff, false) == InvalidOffsetNumber) {
            rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
            securec_check(rc, "", "");
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to add hikey to the right sibling while splitting block %u of index \"%s\"",
                           origpagenumber, RelationGetRelationName(rel))));
        }
        rightoff = OffsetNumberNext(rightoff);
    }

    /*
     * The "high key" for the new left page will be the first key that's going
     * to go into the new right page, or possibly a truncated version if this
     * is a leaf page split.  This might be either the existing data item at
     * position firstright, or the incoming tuple.
     *
     * The high key for the left page is formed using the first item on the
     * right page, which may seem to be contrary to Lehman & Yao's approach of
     * using the left page's last item as its new high key when splitting on
     * the leaf level.  It isn't, though: suffix truncation will leave the
     * left page's high key fully equal to the last item on the left page when
     * two tuples with equal key values (excluding heap TID) enclose the split
     * point.  It isn't actually necessary for a new leaf high key to be equal
     * to the last item on the left for the L&Y "subtree" invariant to hold.
     * It's sufficient to make sure that the new leaf high key is strictly
     * less than the first item on the right leaf page, and greater than or
     * equal to (not necessarily equal to) the last item on the left leaf
     * page.
     *
     * In other words, when suffix truncation isn't possible, L&Y's exact
     * approach to leaf splits is taken.  (Actually, even that is slightly
     * inaccurate.  A tuple with all the keys from firstright but the heap TID
     * from lastleft will be used as the new high key, since the last left
     * tuple could be physically larger despite being opclass-equal in respect
     * of all attributes prior to the heap TID attribute.)
     */
    IndexTuple lastleft;
    if (!noinsert) {
        bool itup_extended = false;
        if (!newitemonleft && newitemoff == firstrightoff) {
            /* incoming tuple will become first on right page */
            itemsz = newitemsz;
            firstright = newitem;
            /*
            * we have expanded the itup size by 8B in multi-version index to represent the
            * space it actually occupies. When the itup is written to the page, the 8B is
            * restored.
            *
            * if we choose newitem as firstright here, it may be copied as HIKEY. So we need
            * to remind the 8B for this case.
            */
            // itup_extended = isleaf; /* expanded only in multi-version index */
        } else {
            /* existing item at firstright will become first on right page */
            itemid = UBTreePCRGetRowPtr(origpage, firstrightoff);
            firstright = (IndexTuple)UBTreePCRGetIndexTupleByItemId(origpage, itemid);
            itemsz = IndexTupleSize(firstright);
        }
    
        /*
        * Truncate unneeded key and non-key attributes of the high key item
        * before inserting it on the left page.  This can only happen at the leaf
        * level, since in general all pivot tuple values originate from leaf
        * level high keys.  A pivot tuple in a grandparent page must guide a
        * search not only to the correct parent page, but also to the correct
        * leaf page.
        */
        if (isleaf) {
            /*
            * Determine which tuple will become the last on the left page.  This
            * is needed to decide how many attributes from the first item on the
            * right page must remain in new high key for left page.
            */
            if (newitemonleft && newitemoff == firstrightoff) {
                /* incoming tuple becomes lastleft */
                lastleft = newitem;
            } else {
                OffsetNumber lastleftoff;

                /* existing item before firstright becomes lastleft */
                lastleftoff = OffsetNumberPrev(firstrightoff);
                Assert(lastleftoff >= P_FIRSTDATAKEY(oopaque));
                itemid = UBTreePCRGetRowPtr(origpage, lastleftoff);
                lastleft = (IndexTuple) UBTreePCRGetIndexTupleByItemId(origpage, itemid);
            }

            /* itup_key is NULL only when inserting parent */
            if (itup_key == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("itup_key for truncate is NULL")));
                return InvalidBuffer;
            }
            lefthighkey = UBTreeTruncate(rel, lastleft, firstright, itup_key, itup_extended);
            itemsz = IndexTupleSize(lefthighkey);
            itemsz = MAXALIGN(itemsz);
        } else {
            /*
            * Don't perform suffix truncation on a copy of firstright to make
            * left page high key for internal page splits.  Must use firstright
            * as new high key directly.
            *
            * Each distinct separator key value originates as a leaf level high
            * key; all other separator keys/pivot tuples are copied from one
            * level down.  A separator key in a grandparent page must be
            * identical to high key in rightmost parent page of the subtree to
            * its left, which must itself be identical to high key in rightmost
            * child page of that same subtree (this even applies to separator
            * from grandparent's high key).  There must always be an unbroken
            * "seam" of identical separator keys that guide index scans at every
            * level, starting from the grandparent.  That's why suffix truncation
            * is unsafe here.
            *
            * Internal page splits will truncate firstright into a "negative
            * infinity" data item when it gets inserted on the new right page
            * below, though.  This happens during the call to _bt_pgaddtup() for
            * the new first data item for right page.  Do not confuse this
            * mechanism with suffix truncation.  It is just a convenient way of
            * implementing page splits that split the internal page "inside"
            * firstright.  The lefthighkey separator key cannot appear a second
            * time in the right page (only firstright's downlink goes in right
            * page).
            */
            lefthighkey = firstright;
        }
    } else {
        IndexTuple firstright = UBTreePCRGetIndexTuple(origpage, firstrightoff);
        /* existing item before firstright becomes lastleft */
        OffsetNumber lastleftoff = OffsetNumberPrev(firstrightoff);
        Assert(lastleftoff >= P_FIRSTDATAKEY(oopaque));
        itemid = UBTreePCRGetRowPtr(origpage, lastleftoff);
        lastleft = (IndexTuple) UBTreePCRGetIndexTupleByItemId(origpage, itemid);
        lefthighkey = UBTreeTruncate(rel, lastleft, firstright, itup_key, false);
        itemsz = IndexTupleSize(lefthighkey);
        itemsz = MAXALIGN(itemsz);
    }
    /*
     * Add new high key to leftpage
     */
    leftoff = P_HIKEY;

    Assert(UBTreeTupleGetNAtts(lefthighkey, rel) > 0);
    Assert(UBTreeTupleGetNAtts(lefthighkey, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
    Assert(itemsz == MAXALIGN(IndexTupleSize(lefthighkey)));


    if (UBTPCRPageAddItem(leftpage, (Item)lefthighkey, itemsz, leftoff, false) == InvalidOffsetNumber) {
        rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
        securec_check(rc, "", "");
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add hikey to the left sibling while splitting block %u of index \"%s\"",
                       origpagenumber, RelationGetRelationName(rel))));
    }
    leftoff = OffsetNumberNext(leftoff);

    /* be tidy */
    if (lefthighkey != firstright) {
        /* this means the lefthighkey is copied from first right and be truncated */
        pfree(lefthighkey);
    }

    /*
     * Now transfer all the data items to the appropriate page.
     *
     * Note: we *must* insert at least the right page's items in item-number
     * order, for the benefit of _bt_restore_page().
     */
    maxoff = UBTreePCRPageGetMaxOffsetNumber(origpage);

    for (i = P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i)) {
        itemid = UBTreePCRGetRowPtr(origpage, i);
        itemsz = ItemIdGetLength(itemid);
        item = (IndexTuple)UBTreePCRGetIndexTupleByItemId(origpage, itemid);

        if (!noinsert) {
            /* does new item belong before this one? */
            if (i == newitemoff) {
                Assert(actualInsertBuf == InvalidBuffer);
                Assert(actualInsertOff == InvalidOffsetNumber);
                if (newitemonleft) {
                    newitemleftoff = leftoff;
                    if (!UBTreePCRPageAddTuple(leftpage, newitemsz, NULL, newitem, leftoff, false, tdslot)) {
                        rc = memset_s(leftpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                        securec_check(rc, "", "");
                        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("failed to add new item to the left sibling while splitting block %u of index \"%s\"",
                                origpagenumber, RelationGetRelationName(rel))));
                    }
                    actualInsertBuf = buf;
                    actualInsertOff = leftoff;
                    leftoff = OffsetNumberNext(leftoff);
                    /* update active hint */
                    lopaque->activeTupleCount++;
                } else {
                    if (!UBTreePCRPageAddTuple(rightpage, newitemsz, NULL, newitem, rightoff, false, tdslot)) {
                        rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                        securec_check(rc, "", "");
                        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("failed to add new item to the right sibling while splitting block %u of index \"%s\"",
                                origpagenumber, RelationGetRelationName(rel))));
                    }
                    actualInsertBuf = rbuf;
                    actualInsertOff = rightoff;
                    rightoff = OffsetNumberNext(rightoff);
                    /* update active hint */
                    ropaque->activeTupleCount++;
                }
            }
        }

        /* decide which page to put it on */
        if (i < firstrightoff) {
            if (!UBTreePCRPageAddTuple(leftpage, itemsz, itemid, item, leftoff, true, tdslot, false)) {
                rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                securec_check(rc, "", "");
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add old item to the left sibling while splitting block %u of index \"%s\"",
                               origpagenumber, RelationGetRelationName(rel))));
            }
            leftoff = OffsetNumberNext(leftoff);
            if (isleaf && !IsUBTreePCRItemDeleted(UBTreePCRGetIndexTupleTrx(item))) {
                lopaque->activeTupleCount++; /* not deleted, count as active */
            }
        } else {
            if (!UBTreePCRPageAddTuple(rightpage, itemsz, itemid, item, rightoff, true, tdslot, false)) {
                rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                securec_check(rc, "", "");
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add old item to the right sibling while splitting block %u of index \"%s\"",
                               origpagenumber, RelationGetRelationName(rel))));
            }
            rightoff = OffsetNumberNext(rightoff);
            if (isleaf && !IsUBTreePCRItemDeleted(UBTreePCRGetIndexTupleTrx(item))) {
                ropaque->activeTupleCount++; /* not deleted, count as active */
            }
        }
    }

    if (!noinsert) {
        /* cope with possibility that newitem goes at the end */
        if (i <= newitemoff) {
            /*
            * Can't have newitemonleft here; that would imply we were told to put
            * *everything* on the left page, which cannot fit (if it could, we'd
            * not be splitting the page).
            */
            Assert(!newitemonleft);
            if (!UBTreePCRPageAddTuple(rightpage, newitemsz, NULL, newitem, rightoff, false, tdslot)) {
                rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                securec_check(rc, "", "");
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add new item to the right sibling while splitting block %u of index \"%s\"",
                            origpagenumber, RelationGetRelationName(rel))));
            }
            Assert(actualInsertBuf == InvalidBuffer);
            Assert(actualInsertOff == InvalidOffsetNumber);
            actualInsertBuf = rbuf;
            actualInsertOff = rightoff;
            rightoff = OffsetNumberNext(rightoff);
            /* update active hint */
            ropaque->activeTupleCount++;
        }
    }

    if (isleaf) {
        // TODO：add undo
        Page page = newitemonleft ? leftpage : rightpage;
        OffsetNumber offset = newitemonleft ? leftoff : rightoff;
        BlockNumber blkno = newitemonleft ? BufferGetBlockNumber(buf) : BufferGetBlockNumber(rbuf);
        TransactionId fxid = GetTopTransactionId();
        UBTreeTD td = UBTreePCRGetTD(page, tdslot);
        td->setInfo(fxid, urec);
        UndoRecord *undorec = (*t_thrd.ustore_cxt.urecvec)[0];
        undorec->SetOffset(offset);
        undorec->SetBlkno(blkno);

        UndoPersistence persistence = UndoPersistenceForRelation(rel);
            /* Insert the Undo record into the undo store */
        InsertPreparedUndo(t_thrd.ustore_cxt.urecvec);
        SetCurrentTransactionUndoRecPtr(urec, persistence);
        undo::PrepareUndoMeta(meta, persistence, t_thrd.ustore_cxt.urecvec->LastRecord(),
            t_thrd.ustore_cxt.urecvec->LastRecordSize());
        undo::UpdateTransactionSlot(fxid, meta, t_thrd.ustore_cxt.urecvec->FirstRecord(), persistence);
    }

    /*
     * We have to grab the right sibling (if any) and fix the prev pointer
     * there. We are guaranteed that this is deadlock-free since no other
     * writer will be holding a lock on that page and trying to move left, and
     * all readers release locks on a page before trying to fetch its
     * neighbors.
     */
    if (!P_RIGHTMOST(oopaque)) {
        sbuf = _bt_getbuf(rel, oopaque->btpo_next, BT_WRITE);
        spage = BufferGetPage(sbuf);
        sopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(spage);
        if (sopaque->btpo_prev != origpagenumber) {
            rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
            securec_check(rc, "", "");

            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("right sibling's left-link doesn't match: block %u links to %u instead of expected %u in "
                           "index \"%s\"",
                           oopaque->btpo_next, sopaque->btpo_prev, origpagenumber, RelationGetRelationName(rel))));
        }

        /*
         * Check to see if we can set the SPLIT_END flag in the right-hand
         * split page; this can save some I/O for vacuum since it need not
         * proceed to the right sibling.  We can set the flag if the right
         * sibling has a different cycleid: that means it could not be part of
         * a group of pages that were all split off from the same ancestor
         * page.  If you're confused, imagine that page A splits to A B and
         * then again, yielding A C B, while vacuum is in progress.  Tuples
         * originally in A could now be in either B or C, hence vacuum must
         * examine both pages.	But if D, our right sibling, has a different
         * cycleid then it could not contain any tuples that were in A when
         * the vacuum started.
         */
        if (!noinsert && sopaque->btpo_cycleid != ropaque->btpo_cycleid) {
            ropaque->btpo_flags |= BTP_SPLIT_END;
        }
    }

    /*
     * Right sibling is locked, new siblings are prepared, but original page
     * is not updated yet.
     *
     * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
     * not starting the critical section till here because we haven't been
     * scribbling on the original page yet; see comments above.
     */
    START_CRIT_SECTION();

    /*
     * By here, the original data page has been split into two new halves, and
     * these are correct.  The algorithm requires that the left page never
     * move during a split, so we copy the new left page back on top of the
     * original.  Note that this is not a waste of time, since we also require
     * (in the page management code) that the center of a page always be
     * clean, and the most efficient way to guarantee this is just to compact
     * the data by reinserting it into a new left page.  (XXX the latter
     * comment is probably obsolete; but in any case it's good to not scribble
     * on the original page until we enter the critical section.)
     *
     * We need to do this before writing the WAL record, so that XLogInsert
     * can WAL log an image of the page if necessary.
     */
    PageRestoreTempPage(leftpage, origpage);
    /* leftpage, lopaque must not be used below here */
    MarkBufferDirty(buf);
    MarkBufferDirty(rbuf);

    if (!P_RIGHTMOST(ropaque)) {
        sopaque->btpo_prev = rightpagenumber;
        MarkBufferDirty(sbuf);
    }

    /*
     * Clear INCOMPLETE_SPLIT flag on child if inserting the new item finishes
     * a split.
     */
    if (!isleaf) {
        Page cpage = BufferGetPage(cbuf);
        UBTPCRPageOpaque cpageop = (UBTPCRPageOpaque)PageGetSpecialPointer(cpage);

        cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
        MarkBufferDirty(cbuf);
    }


    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        // TODO: 补充xlog
        // ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
        //         errmsg("TODO: 补充xlog")));
    }
    if (isleaf) {
        undo::FinishUndoMeta(UndoPersistenceForRelation(rel));
    }
    END_CRIT_SECTION();
    // Page page = BufferGetPage(actualInsertBuf);
    // UBTreeVerify(rel, page, BufferGetBlockNumber(actualInsertBuf), actualInsertOff, true);

    /* discard this page from the Recycle Queue */
    UBTreePCRRecordUsedPage(rel, addr);

    /* release the old right sibling */
    if (!P_RIGHTMOST(ropaque)) {
        _bt_relbuf(rel, sbuf);
    }
    /* release the child */
    if (!isleaf) {
        _bt_relbuf(rel, cbuf);
    }

    /* split's done */
    return rbuf;
}

/*
 *	UBTreePCRPageAddTuple() -- add a tuple to a particular page in the index.
 *
 *		This routine adds the tuple to the page as requested.  It does
 *		not affect pin/lock status, but you'd better have a write lock
 *		and pin on the target buffer!  Don't forget to write and release
 *		the buffer afterwards, either.
 *
 *		The main difference between this routine and a bare PageAddItem call
 *		is that this code knows that the leftmost index tuple on a non-leaf
 *		btree page doesn't need to have a key.  Therefore, it strips such
 *		tuples down to just the tuple header.  CAUTION: this works ONLY if
 *		we insert the tuples in order, so that the given itup_off does
 *		represent the final position of the tuple!
 */
static bool UBTreePCRPageAddTuple(Page page, Size itemsize, ItemId iid, IndexTuple itup, OffsetNumber itup_off, 
    bool copyflags, uint8 tdslot, bool isnew)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTDATAKEY(opaque)) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        UBTreeTupleSetNAtts(&trunctuple, 0, false);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (! isnew || !(P_ISLEAF(opaque))) {
        /* if isnew and !P_HAS_XMIN_XMAX, it must inserting downlink to parent and itup is normal, no special handle
         * needed */
        if (UBTPCRPageAddItem(page, (Item)itup, itemsize, itup_off, false) == InvalidOffsetNumber) {
            return false;
        }
        return true;
    }
    
    if (isnew) {
        itemsize -= sizeof(IndexTupleTrxData);
        IndexTupleSetSize(itup, itemsize);
    }  
    
    /*
    * New inserted tuple need mark xmin, except HIKEY.
    * We assume itemsize included the reserved space for xmin/xmax.
    */
    IndexTupleTrx trx = UBTreePCRGetIndexTupleTrx(itup);
    ((PageHeader)page)->pd_upper -= MAXALIGN(sizeof(IndexTupleTrxData));
    itup_off = UBTPCRPageAddItem(page, (Item)itup, itemsize, itup_off, false);
    if (itup_off == InvalidOffsetNumber) {
        /* restore page->upper before we return false */
        ((PageHeader)page)->pd_upper += MAXALIGN(sizeof(IndexTupleTrxData));
        return false;
    }

    ItemId curiid = UBTreePCRGetRowPtr(page, itup_off);
    IndexTuple curtup = UBTreePCRGetIndexTupleByItemId(page, curiid);
    IndexTupleTrx curtrx = UBTreePCRGetIndexTupleTrx(curtup);
    
    if (copyflags) {
        curtrx->tdSlot = tdslot;
        curtrx->slotIsInvalid = trx->slotIsInvalid;
        curtrx->isDeleted = trx->isDeleted;
        curiid->lp_flags = iid->lp_flags;
    } else {
        curtrx->tdSlot = tdslot;
        curtrx->slotIsInvalid = 0;
        curtrx->isDeleted = 0;
    }

    return true;
}

OffsetNumber UBTreePCRFindDeleteLoc(Relation rel, Buffer* bufP, OffsetNumber offset, BTScanInsert itup_key, IndexTuple itup)
{
    OffsetNumber maxoff;
    Page page;
    UBTPCRPageOpaque opaque;

    page = BufferGetPage(*bufP);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
    Oid partOid = InvalidOid;

    /* Get partOid for global partition index */
    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        TupleDesc tupdesc = RelationGetDescr(rel);
        partOid = DatumGetUInt32(index_getattr(itup, partitionOidAttr, tupdesc, &isnull));
        Assert(!isnull);
    }

    /* Scan over all equal tuples, looking for itup */
    for (;;) {
        ItemId curitemid;
        IndexTuple curitup;
        BlockNumber nblkno;

        if (offset <= maxoff) {
            curitemid = UBTreePCRGetRowPtr(page, offset);
            if (!ItemIdIsDead(curitemid)) {
                /* 
                 * _bt_compare returns 0 for (1,NULL) and (1, NULL) - this's 
                 * how we handling NULLs - and so we must not use _bt_compare
                 * in real comparison, but only for ording/finding items on
                 * pages.
                 */
                if (!UBTreePCRIsEqual(rel, page, offset, itup_key->keysz, itup_key->scankeys)) {
                    /* 
                     * we've traversed all the equal tuples, but we haven't found itup
                     */
                    return InvalidOffsetNumber;
                }
                curitup = (IndexTuple)UBTreePCRGetIndexTuple(page, offset);
                if (UBTreeHasIncluding(rel)) {          // diff
                    Size itupSize = IndexTupleSize(itup);
                    if (itupSize == IndexTupleSize(curitup) && memcmp(curitup, itup, itupSize) == 0) {
                        return offset;
                    }
                } else if (UBTreePCRIndexTupleMatches(rel, page, offset, itup, NULL, partOid)) {        
                    return offset;
                }
            }
        }

        /*
         * Advance to the next tuple to continue checking
         */
        if (offset < maxoff) {
            offset = OffsetNumberNext(offset);
        } else {
            int highkeycmp;

            /* 
             * if scankey == highkey we gotta check the next page too
             */
            if (P_RIGHTMOST(opaque)) {
                break;
            }
            highkeycmp = UBTreePCRCompare(rel, itup_key, page, P_HIKEY);
            Assert(highkeycmp <= 0);
            if (highkeycmp != 0) {
                break;
            }
            /* 
             * Check the next non-dead page if scankey equals to the current page's highkey
             */
            for (;;) {
                nblkno = opaque->btpo_next;
                *bufP = _bt_relandgetbuf(rel, *bufP, nblkno, BT_WRITE);
                page = BufferGetPage(*bufP);
                opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
                if (!P_IGNORE(opaque)) {
                    break;
                }
                if (P_RIGHTMOST(opaque)) {
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\" at blkno %u",
                                    RelationGetRelationName(rel), nblkno)));
                }
            }
            maxoff = UBTreePCRPageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }
    /*
     * No corresponding itup was found, release buffer and return InvalidOffsetNumber
     */
    return InvalidOffsetNumber;
}

// UBTreePCRIndexTupleMatches(rel, page, offset, itup, NULL, partOid)
/* return true if the curItup is exactly what we want (keys, tid, partOid, etc.)*/
bool UBTreePCRIndexTupleMatches(Relation rel, Page page, OffsetNumber offnum, IndexTuple target_itup, BTScanInsert itup_key, Oid target_part_oid) {
    IndexTuple curItup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    if (itup_key && !UBTreePCRIsEqual(rel, page, offnum, itup_key->keysz, itup_key->scankeys)) {
        return false;
    }
    if (!ItemPointerEquals(&curItup->t_tid, &target_itup->t_tid)) {
        return false;
    }
    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        TupleDesc tupdesc = RelationGetDescr(rel);
        AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
        Oid cur_part_oid = DatumGetUInt32(index_getattr(curItup, partitionOidAttr, tupdesc, &isnull));
        if (cur_part_oid != target_part_oid) {
            return false;
        }
    }
    return true;
}

uint8 PreparePCRDelete(Relation rel, Buffer buf, OffsetNumber offnum, UBTreeUndoInfo undoInfo, TransactionId* minXid, bool* needRetry)
{
    /* Get TD slot */
    UBTreeTDData oldTDData;

    TransactionId fxid = GetTopTransactionId();       
    uint8 tdslot;
    tdslot = UBTreePageReserveTransactionSlot(rel, buf, fxid, &oldTDData, minXid);
    if (tdslot == UBTreeInvalidTDSlotId) {
        return UBTreeInvalidTDSlotId;
    }

    /* Get tuple's previouse operation's xact info */
    Page page = BufferGetPage(buf);
    ItemId iid = (ItemId)UBTreePCRGetRowPtr(page, offnum);
    IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    IndexTupleTrx trx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
    uint8 slotNo = trx->tdSlot;
    UBTreePCRHandlePreviousTD(rel, buf, &slotNo, trx, needRetry);       // TODO : 并发事物处理 
    if (*needRetry) {
        return tdslot;
    }

    UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo);
    
    if (UBTreePCRTDIdIsNormal(slotNo)) {    
        TransactionId xid = thisTrans->xactid;
        TransactionId oldestXmin = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
        if (TransactionIdIsValid(xid) && TransactionIdPrecedes(xid, oldestXmin)) {
            IndexItemIdSetFrozen(iid);
            UBTreePCRSetIndexTupleTrxSlot(trx, UBTreeFrozenTDSlotId);
            slotNo = UBTreeFrozenTDSlotId;
        }
    }

    if (IsUBTreePCRItemDeleted(trx)) {      
        ereport(PANIC,(errmodule(MOD_UBTREE),
                errmsg("failed to delete index tuple, index \"%s\", blkno %u", RelationGetRelationName(rel), BufferGetBlockNumber(buf)),
                errcause("UBTree PCR page is corrupted"),
                erraction("Please try to reindex to fix it")));
    }

    TransactionId prevXid = FrozenTransactionId;
    if (slotNo != UBTreeFrozenTDSlotId) {
        thisTrans = UBTreePCRGetTD(page, slotNo);
        if (thisTrans->xactid == fxid) {
            /* self delete case */
            prevXid = fxid;
        } else if (thisTrans->xactid != InvalidTransactionId) {
            /* must be current xid or committed txn */
            prevXid = thisTrans->xactid;  
        }
    }

    undoInfo->prev_td_id = slotNo;

    /* Update undo record */
    URecVector *urecvec = t_thrd.ustore_cxt.urecvec;
    UndoRecord *urec = (*urecvec)[0];
    urec->SetBlkprev(oldTDData.undoRecPtr);
    urec->SetOldXactId(prevXid);
    urec->SetOffset(offnum);
    return tdslot;
}

UndoRecPtr UBTreePCRPrepareUndoDelete(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneXact, 
    IndexTuple oldtuple, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa, UBTreeUndoInfo undoInfo)
{
    URecVector *urecvec = t_thrd.ustore_cxt.urecvec;
    UndoRecord *urec = (*urecvec)[0];

    urec->SetUtype(UNDO_UBT_DELETE);
    urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetBlkprev(prevurpInOneXact);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);

    if (t_thrd.xlog_cxt.InRecovery) {
        urec->SetBlkno(blk);
    } else {
        if (BufferIsValid(buffer)) {
            urec->SetBlkno(BufferGetBlockNumber(buffer));
        } else {
            urec->SetBlkno(InvalidBlockNumber);
        }
    }

    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetNeedInsert(true);

    /* Copy over the entire tuple data to the undorecord */
    MemoryContext oldCxt = MemoryContextSwitchTo(urec->mem_context());
    initStringInfo(urec->Rawdata());
    MemoryContextSwitchTo(oldCxt);
    appendBinaryStringInfo(urec->Rawdata(), (char *)undoInfo, SizeOfUBTreeUndoInfoData);
    appendBinaryStringInfo(urec->Rawdata(), (char *)oldtuple, IndexTupleSize(oldtuple));

    int status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);

    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    UndoRecPtr urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    return urecptr;
}

/*
 * UBTreeIsEqual() -- used in _bt_doinsert in check for duplicates.
 *
 * This is very similar to _bt_compare, except for NULL and negative infinity
 * handling. Rule is simple: NOT_NULL not equal NULL, NULL equal NULL.
 */
bool UBTreePCRIsEqual(Relation idxrel, Page page, OffsetNumber offnum, int keysz, ScanKey scankey)
{
    TupleDesc itupdesc = RelationGetDescr(idxrel);
    IndexTuple itup;
    int i;

    /* Better be comparing to a leaf item */
    Assert(P_ISLEAF((UBTPCRPageOpaque)PageGetSpecialPointer(page)));
    Assert(offnum >= P_FIRSTDATAKEY((UBTPCRPageOpaque) PageGetSpecialPointer(page)));

    itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    /*
     * Index tuple shouldn't be truncated.	Despite we technically could
     * compare truncated tuple as well, this function should be only called
     * for regular non-truncated leaf tuples and P_HIKEY tuple on
     * rightmost leaf page.
     */
    for (i = 1; i <= keysz; i++, scankey++) {
        AttrNumber attno = scankey->sk_attno;
        Assert(attno == i);
        bool datumIsNull = false;
        bool skeyIsNull = ((scankey->sk_flags & SK_ISNULL) ? true : false);
        Datum datum = index_getattr(itup, attno, itupdesc, &datumIsNull);

        if (datumIsNull && skeyIsNull)
            continue; /* NULL equal NULL */
        if (datumIsNull != skeyIsNull)
            return false; /* NOT_NULL not equal NULL */

        if (DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
                                            scankey->sk_collation, datum, scankey->sk_argument)) != 0) {
            return false;
        }
    }

    /* if we get here, the keys are equal */
    return true;
}

static void UBTreePCRSplitForTD(Relation rel, Buffer buf, BTScanInsert itup_key, BTStack stack)
{
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    OffsetNumber splitOffset = UBTreePCRFindsplitloc(rel, buf, P_FIRSTDATAKEY(opaque), 0, NULL);

    Buffer right = UBTreePCRSplit(rel, buf, InvalidBuffer, splitOffset, InvalidOffsetNumber, 
                                0,  NULL, false, itup_key, UBTreeInvalidTDSlotId, INVALID_UNDO_REC_PTR, NULL, true);
    bool is_root = P_ISROOT(opaque) > 0 ? true : false;
    bool is_only = P_LEFTMOST(opaque) && P_RIGHTMOST(opaque);
    PredicateLockPageSplit(rel, BufferGetBlockNumber(buf), BufferGetBlockNumber(right));

    UBTreePCRInsertParent(rel, buf, right, stack, is_root, is_only);

    /* ubtree will try to recycle one page after allocate one page */
    UBTreePCRTryRecycleEmptyPage(rel);
}

void UBTreePCROrWaitForTDSlot(TransactionId xWait, bool isInsert)
{
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_AVAILABLE_TD);
    if (!t_thrd.ustore_cxt.tdSlotWaitActive) {
        Assert(t_thrd.ustore_cxt.tdSlotWaitFinishTime == 0);
        t_thrd.ustore_cxt.tdSlotWaitFinishTime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
            TD_RESERVATION_TIMEOUT_MS);
        t_thrd.ustore_cxt.tdSlotWaitActive = true;
    }

    TimestampTz now = GetCurrentTimestamp();
    if (t_thrd.ustore_cxt.tdSlotWaitFinishTime <= now) {
            Assert(TransactionIdIsValid(xWait));
            XactLockTableWait(xWait);
    } else if (!isInsert) {
        pg_usleep(10000L);
    }
    pgstat_report_waitstatus(oldStatus);
}

/*
 *	UBTreePCRNewRoot() -- Create a new root page for the index.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
static Buffer UBTreePCRNewRoot(Relation rel, Buffer lbuf, Buffer rbuf)
{
    Buffer rootbuf;
    Page lpage, rootpage;
    BlockNumber lbkno, rbkno;
    BlockNumber rootblknum;
    UBTPCRPageOpaque rootopaque;
    UBTPCRPageOpaque lopaque;
    ItemId itemid;
    IndexTuple item;
    IndexTuple left_item;
    Size left_item_sz;
    IndexTuple right_item;
    Size right_item_sz;
    Buffer metabuf;
    Page metapg;
    BTMetaPageData *metad = NULL;

    lbkno = BufferGetBlockNumber(lbuf);
    rbkno = BufferGetBlockNumber(rbuf);
    lpage = BufferGetPage(lbuf);
    lopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(lpage);

    /*
     *	We have split the old root page and need to create a new one.
     *	Here we can not have any ERROR before the new root is created,
     *	and the metapage is update.
     *
     *	If we meet the ERROR here, the old root has been split into two
     *	pages, and the new root is failed to create. In this situation, the B+
     *	tree is without the root page, and the metadata is wrong.
     *
     *	1. root 1 splits to 1 and 2, block 1's root flag has changed.
     *	2. when we get in this function and haven't updated metapage's
     *	 rootpointer. ERROR here and we release our LWlock on 1 and 2.
     *	3. we continue insert into 1 and block 1 splits again. we use outdated
     *	 metapage to find 'trueroot' . deaklock.
     *
     *	Don't worry about incomplete btree pages as a result of PANIC here,
     *	because we can complete this incomplete action in btree_xlog_cleanup
     *	after redo our xlog.
     */
    START_CRIT_SECTION();

    /*
     * get a new root page
     *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
     *            before we release the Exclusive lock.
     */
    UBTRecycleQueueAddress addr;
    rootbuf = UBTreePCRGetNewPage(rel, &addr);
    rootpage = BufferGetPage(rootbuf);
    rootblknum = BufferGetBlockNumber(rootbuf);

    /* acquire lock on the metapage */
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
    metapg = BufferGetPage(metabuf);
    metad = BTPageGetMeta(metapg);

    /*
     * Create downlink item for left page (old root).  Since this will be the
     * first item in a non-leaf page, it implicitly has minus-infinity key
     * value, so we need not store any actual key in it.
     */
    left_item_sz = sizeof(IndexTupleData);
    left_item = (IndexTuple)palloc(left_item_sz);
    left_item->t_info = (unsigned short)left_item_sz;

    UBTreeTupleSetDownLink(left_item, lbkno);
    UBTreeTupleSetNAtts(left_item, 0, false);

    /*
     * Create downlink item for right page.  The key for it is obtained from
     * the "high key" position in the left page.
     */
    itemid = UBTreePCRGetRowPtr(lpage, P_HIKEY);
    item = UBTreePCRGetIndexTupleByItemId(lpage, itemid);
    right_item = CopyIndexTuple(item);

    UBTreeTupleSetDownLink(right_item, rbkno);
    right_item_sz = IndexTupleSize(right_item);

    /* set btree special data */
    rootopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rootpage);
    rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
    rootopaque->btpo_flags = BTP_ROOT;
    rootopaque->btpo.level = ((UBTPCRPageOpaque)PageGetSpecialPointer(lpage))->btpo.level + 1;
    rootopaque->btpo_cycleid = 0;

    /* update metapage data */
    metad->btm_root = rootblknum;
    metad->btm_level = rootopaque->btpo.level;
    metad->btm_fastroot = rootblknum;
    metad->btm_fastlevel = rootopaque->btpo.level;
    rel->rd_rootcache = InvalidBuffer;

    /*
     * Insert the left page pointer into the new root page.  The root page is
     * the rightmost page on its level so there is no "high key" in it; the
     * two items will go into positions P_HIKEY and P_FIRSTKEY.
     *
     * Note: we *must* insert the two items in item-number order, for the
     * benefit of _bt_restore_page().
     */
    if (UBTPCRPageAddItem(rootpage, (Item)left_item, left_item_sz, P_HIKEY, false) == InvalidOffsetNumber) {
        ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add leftkey to new root page while splitting block %u of index \"%s\"",
                       BufferGetBlockNumber(lbuf), RelationGetRelationName(rel))));

    }

    /*
     * insert the right page pointer into the new root page.
     */
    if (UBTPCRPageAddItem(rootpage, (Item)right_item, right_item_sz, P_FIRSTKEY, false) == InvalidOffsetNumber) {
        ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add rightkey to new root page while splitting block %u of index \"%s\"",
                       BufferGetBlockNumber(lbuf), RelationGetRelationName(rel))));
    }

    /* Clear the incomplete-split flag in the left child */
    Assert(P_INCOMPLETE_SPLIT(lopaque));
    lopaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
    MarkBufferDirty(lbuf);

    MarkBufferDirty(rootbuf);
    MarkBufferDirty(metabuf);

    // /* XLOG stuff */
    // if (RelationNeedsWAL(rel)) {
    //     xl_btree_newroot xlrec;
    //     XLogRecPtr recptr;
    //     xl_btree_metadata_old md;

    //     xlrec.rootblk = rootblknum;
    //     xlrec.level = metad->btm_level;

    //     Assert(xlrec.level > 0);

    //     XLogBeginInsert();
    //     XLogRegisterData((char *)&xlrec, SizeOfBtreeNewroot);

    //     XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
    //     XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
    //     XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

    //     md.root = rootblknum;
    //     md.level = metad->btm_level;
    //     md.fastroot = rootblknum;
    //     md.fastlevel = metad->btm_level;

    //     XLogRegisterBufData(2, (char *)&md, sizeof(xl_btree_metadata_old));

    //     /*
    //      * Direct access to page is not good but faster - we should implement
    //      * some new func in page API.
    //      */
    //     XLogRegisterBufData(0, (char *)rootpage + ((PageHeader)rootpage)->pd_upper,
    //                         ((PageHeader)rootpage)->pd_special - ((PageHeader)rootpage)->pd_upper);

    //     recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_NEWROOT);

    //     PageSetLSN(lpage, recptr);
    //     PageSetLSN(rootpage, recptr);
    //     PageSetLSN(metapg, recptr);
    // }

    END_CRIT_SECTION();
    // UBTreeVerify(rel, rootpage, rootblknum);

    /* done with metapage */
    _bt_relbuf(rel, metabuf);

    /* discard this page from the Recycle Queue */
    UBTreePCRRecordUsedPage(rel, addr);

    pfree(left_item);
    pfree(right_item);

    return rootbuf;
}
