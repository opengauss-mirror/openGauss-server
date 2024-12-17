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
 * ubtpcrpage.cpp
 *       BTree pcr specific page management code for the openGauss ubtree access method.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrpage.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"
#include "access/multi_redo_api.h"
#include "access/hio.h"
#include "storage/lmgr.h"
#include "catalog/pg_partition_fn.h"

void UBTreePCRInitTD(Page page)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    opaque->td_count = UBTREE_DEFAULT_TD_COUNT;
    ((PageHeader)page)->pd_lower += SizeOfUBTreeTDData(page);
    for (int i = 1; i <= opaque->td_count; i++) {
        UBTreeTD td = UBTreePCRGetTD(page, i);
        UBTreePCRTDSetStatus(td, TD_FROZEN);
    }
}

/*
 *	UBTreePCRPageInit() -- Initialize a new page.
 *
 * On return, the page header is initialized; data space is empty;
 * special space is zeroed out.
 */
void UBTreePCRPageInit(Page page, Size size)
{
    PageInit(page, size, sizeof(UBTPCRPageOpaqueData));
    ((UBTPCRPageOpaque)PageGetSpecialPointer(page))->last_delete_xid = 0;
}

/*
 *	UBTreePCRInitMetaPage() -- Fill a page buffer with a correct metapage image
 */
void UBTreePCRInitMetaPage(Page page, BlockNumber rootbknum, uint32 level)
{
    BTMetaPageData *metad = NULL;
    UBTPCRPageOpaque metaopaque;

    UBTreePCRPageInit(page, BLCKSZ);

    metad = BTPageGetMeta(page);
    metad->btm_magic = BTREE_MAGIC;
    metad->btm_version = UBTREE_PCR_VERSION;
    metad->btm_root = rootbknum;
    metad->btm_level = level;
    metad->btm_fastroot = rootbknum;
    metad->btm_fastlevel = level;

    metaopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    metaopaque->btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.	This is not essential
     * but it makes the page look compressible to xlog.c.
     */
    ((PageHeader)page)->pd_lower = (uint16)(((char *)metad + sizeof(BTMetaPageData)) - (char *)page);
}


Buffer UBTreePCRGetRoot(Relation rel, int access)
{
    Buffer metabuf;
    Page metapg;
    UBTPCRPageOpaque metaopaque;
    Buffer rootbuf;
    Page rootpage;
    UBTPCRPageOpaque rootopaque;
    BlockNumber rootblkno;
    uint32 rootlevel;
    BTMetaPageData *metad = NULL;

    /*
     * Try to use previously-cached metapage data to find the root.  This
     * normally saves one buffer access per index search, which is a very
     * helpful savings in bufmgr traffic and hence contention.
     */
    if (rel->rd_amcache != NULL) {
        bool isRootCacheValid = false;
        metad = (BTMetaPageData*)rel->rd_amcache;
        /* We shouldn't have cached it if any of these fail */
        Assert(metad->btm_magic == BTREE_MAGIC);
        Assert(metad->btm_version == UBTREE_PCR_VERSION);
        Assert(metad->btm_root != P_NONE);

        rootblkno = metad->btm_fastroot;
        Assert(rootblkno != P_NONE);
        rootlevel = metad->btm_fastlevel;

        /*
         * Great than 0 means shared buffer. Global temp table does not suppport this optimization.
         * Be careful: temp tables use shared buffer, please refer to RelationBuildLocalRelation to
         * check the value of rel->rd_backend
         */
        if (likely(rel->rd_rootcache > 0)) {
            bool valid = false;
            // use cached value, but with double checks to make sure
            rootbuf = rel->rd_rootcache;
            Assert(rootblkno != P_NEW);

            // below is copied from ReadBuffer
            RelationOpenSmgr(rel);

            /* Make sure we will have room to remember the buffer pin */
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

            BufferDesc *buf = GetBufferDescriptor(rootbuf - 1); // caveat for GetBufferDescriptor for -1!
            valid = PinBuffer(buf, NULL);
            if (valid && TryLockBuffer(rootbuf, BT_READ, false)) {
                isRootCacheValid = (buf->tag.forkNum == MAIN_FORKNUM) &&
                    RelFileNodeEquals(buf->tag.rnode, rel->rd_node) && (buf->tag.blockNum == rootblkno);
                if (!isRootCacheValid) {
                    UnlockReleaseBuffer(rootbuf);
                }
            } else {
                UnpinBuffer(buf, true);
            }
        }
        if (!isRootCacheValid) {
            rootbuf = _bt_getbuf(rel, rootblkno, BT_READ);
            // cache it
            rel->rd_rootcache = rootbuf;

            /* BM_IS_ROOT is not used nor unset. Dont set it for now. */
            // prevent it from page out
        }

        rootpage = BufferGetPage(rootbuf);
        rootopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rootpage);
        /*
         * Since the cache might be stale, we check the page more carefully
         * here than normal.  We *must* check that it's not deleted. If it's
         * not alone on its level, then we reject too --- this may be overly
         * paranoid but better safe than sorry.  Note we don't check P_ISROOT,
         * because that's not set in a "fast root".
         */
        if (!P_IGNORE(rootopaque) && rootopaque->btpo.level == rootlevel && P_LEFTMOST(rootopaque) &&
            P_RIGHTMOST(rootopaque)) {
            /* OK, accept cached page as the root */
            return rootbuf;
        }
        _bt_relbuf(rel, rootbuf);
        /* Cache is stale, throw it away */
        if (rel->rd_amcache)
            pfree(rel->rd_amcache);
        rel->rd_amcache = NULL;
        rel->rd_rootcache = InvalidBuffer;
    }

    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    metapg = BufferGetPage(metabuf);
    metaopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(metapg);
    metad = BTPageGetMeta(metapg);
    /* sanity-check the metapage */
    if (!(metaopaque->btpo_flags & BTP_META) || metad->btm_magic != BTREE_MAGIC)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("index \"%s\" is not a btree", RelationGetRelationName(rel))));

    if (metad->btm_version != UBTREE_PCR_VERSION)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("version mismatch in index \"%s\": file version %u, code version %d",
                       RelationGetRelationName(rel), metad->btm_version, UBTREE_PCR_VERSION)));

    /* if no root page initialized yet, do it */
    if (metad->btm_root == P_NONE) {
        /* If access = BT_READ, caller doesn't want us to create root yet */
        if (access == BT_READ) {
            _bt_relbuf(rel, metabuf);
            return InvalidBuffer;
        }

        /* trade in our read lock for a write lock */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(metabuf, BT_WRITE);

        /*
         * Race condition:	if someone else initialized the metadata between
         * the time we released the read lock and acquired the write lock, we
         * must avoid doing it again.
         */
        if (metad->btm_root != P_NONE) {
            /*
             * Metadata initialized by someone else.  In order to guarantee no
             * deadlocks, we have to release the metadata page and start all
             * over again.	(Is that really true? But it's hardly worth trying
             * to optimize this case.)
             */
            _bt_relbuf(rel, metabuf);
            return UBTreePCRGetRoot(rel, access);
        }

        /*
         * Get, initialize, write, and leave a lock of the appropriate type on
         * the new root page.  Since this is the first page in the tree, it's
         * a leaf as well as the root.
         *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
         *            before we release the Exclusive lock.
         */
        UBTRecycleQueueAddress addr;
        rootbuf = UBTreePCRGetNewPage(rel, &addr);
        UBTreePCRInitTD(BufferGetPage(rootbuf));
        rootblkno = BufferGetBlockNumber(rootbuf);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rootpage);
        rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
        rootopaque->btpo_flags = (BTP_LEAF | BTP_ROOT);
        rootopaque->btpo.level = 0;
        rootopaque->btpo_cycleid = 0;

        /* NO ELOG(ERROR) till meta is updated */
        START_CRIT_SECTION();

        metad->btm_root = rootblkno;
        metad->btm_level = 0;
        metad->btm_fastroot = rootblkno;
        metad->btm_fastlevel = 0;

        MarkBufferDirty(rootbuf);
        MarkBufferDirty(metabuf);

        /* XLOG stuff */
        // TODO
        if (RelationNeedsWAL(rel)) {
            // xl_btree_newroot xlrec;
            // xl_btree_metadata_old md;
            // XLogRecPtr recptr;

            // XLogBeginInsert();
            // XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
            // XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT);

            // md.root = rootblkno;
            // md.level = 0;
            // md.fastroot = rootblkno;
            // md.fastlevel = 0;

            // XLogRegisterBufData(2, (char *)&md, sizeof(xl_btree_metadata_old));

            // xlrec.rootblk = rootblkno;
            // xlrec.level = 0;
            // XLogRegisterData((char *)&xlrec, SizeOfBtreeNewroot);

            // recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_NEWROOT);

            // PageSetLSN(rootpage, recptr);
            // PageSetLSN(metapg, recptr);
        }

        END_CRIT_SECTION();

        /* discard this page from the Recycle Queue */
        UBTreeRecordUsedPage(rel, addr);

        /*
         * swap root write lock for read lock.	There is no danger of anyone
         * else accessing the new root page while it's unlocked, since no one
         * else knows where it is yet.
         */
        LockBuffer(rootbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(rootbuf, BT_READ);

        /* okay, metadata is correct, release lock on it */
        _bt_relbuf(rel, metabuf);
        rel->rd_rootcache = InvalidBuffer;
    } else {
        rootblkno = metad->btm_fastroot;
        Assert(rootblkno != P_NONE);
        rootlevel = metad->btm_fastlevel;

        /*
         * Cache the metapage data for next time
         */
        rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt, sizeof(BTMetaPageData));
        errno_t rc = memcpy_s(rel->rd_amcache, sizeof(BTMetaPageData), metad, sizeof(BTMetaPageData));
        securec_check(rc, "", "");

        rel->rd_rootcache = InvalidBuffer;
        /*
         * We are done with the metapage; arrange to release it via first
         * _bt_relandgetbuf call
         */
        rootbuf = metabuf;

        for (;;) {
            rootbuf = _bt_relandgetbuf(rel, rootbuf, rootblkno, BT_READ);
            rootpage = BufferGetPage(rootbuf);
            rootopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rootpage);
            if (!P_IGNORE(rootopaque))
                break;

            /* it's dead, Jim.  step right one page */
            if (P_RIGHTMOST(rootopaque))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("no live root page found in index \"%s\"", RelationGetRelationName(rel))));
            rootblkno = rootopaque->btpo_next;
        }

        /* Note: can't check btpo.level on deleted pages */
        if (rootopaque->btpo.level != rootlevel)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("root page %u of index \"%s\" has level %u, expected %u", rootblkno,
                           RelationGetRelationName(rel), rootopaque->btpo.level, rootlevel)));
    }

    /*
     * By here, we have a pin and read lock on the root page, and no lock set
     * on the metadata page.  Return the root page's buffer.
     */
    return rootbuf;
}

bool UBTreePCRPageRecyclable(Page page)
{
    /*
     * It's possible to find an all-zeroes page in an index --- for example, a
     * backend might successfully extend the relation one page and then crash
     * before it is able to make a WAL entry for adding the page. If we find a
     * zeroed page then reclaim it.
     */
    TransactionId oldestXmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    if (PageIsNew(page)) {
        return true;
    }

    /*
     * Otherwise, recycle if deleted and too old to have any processes
     * interested in it.
     */
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    return P_ISDELETED(opaque) && TransactionIdPrecedes(((UBTPCRPageOpaque)opaque)->last_delete_xid, oldestXmin);
}

void UBTreePCRDeleteOnPage(Relation rel, Buffer buf, OffsetNumber offset, bool isRollbackIndex, int tdslot,
    UndoRecPtr urecPtr, undo::XlogUndoMeta *xlumPtr)
{
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offset);
    IndexTupleTrx trx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
    TransactionId fxid = GetTopTransactionId();
    TransactionId xid = GetCurrentTransactionId();

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    Assert(tdslot != UBTreeInvalidTDSlotId);
    Assert(urecPtr != INVALID_UNDO_REC_PTR);
    Assert(xlumPtr != NULL);

    UBTreePCRSetIndexTupleDeleted(trx);
    UBTreePCRSetIndexTupleTrxValid(trx);
    trx->tdSlot = tdslot;

    UBTreeTD thisTrans = UBTreePCRGetTD(page, tdslot);
    thisTrans->xactid = fxid;
    thisTrans->undoRecPtr = urecPtr;
    thisTrans->combine.csn = InvalidCommitSeqNo;
    thisTrans->tdStatus &= ~(TD_COMMITED | TD_FROZEN | TD_CSN);
    thisTrans->tdStatus |= TD_ACTIVE;
    thisTrans->tdStatus |= TD_DELETE;

    /*
     * Update the UndoRecord now that we know where the tuple is located on the Page.
     * Note: offset of the index tuple may change as all index tuples are ordered on Page.
     */
    URecVector *urecvec = t_thrd.ustore_cxt.urecvec;
    UndoRecord *urec = (*urecvec)[0];
    urec->SetOffset(offset);
    urec->SetBlkno(BufferGetBlockNumber(buf));

    /*
     * insert the Undo record into the undo store
     */
    UndoPersistence persistence = UndoPersistenceForRelation(rel);
    InsertPreparedUndo(t_thrd.ustore_cxt.urecvec);
    UndoRecPtr oldPrevUrp = GetCurrentTransactionUndoRecPtr(persistence);
    SetCurrentTransactionUndoRecPtr(urecPtr, persistence);

    undo::PrepareUndoMeta(xlumPtr, persistence, t_thrd.ustore_cxt.urecvec->LastRecord(),
        t_thrd.ustore_cxt.urecvec->LastRecordSize());
    undo::UpdateTransactionSlot(fxid, xlumPtr, t_thrd.ustore_cxt.urecvec->FirstRecord(), persistence);

    /* update active hint */
    opaque->activeTupleCount--;
    if (TransactionIdPrecedes(opaque->last_delete_xid, xid))
        opaque->last_delete_xid = xid;

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_ubtree3_insert_or_delete xlrec;
        XLogRecPtr recptr;
        XlUndoHeader xlundohdr;
        uint8 xlUndoHeaderFlag = 0;
        TransactionId currentXid = InvalidTransactionId;
        UBTreeUndoInfo undoInfo = (UBTreeUndoInfo)(urec->rawdata_.data);

        Oid relOid = RelationIsPartition(rel) ? GetBaseRelOidOfParition(rel) : RelationGetRelid(rel);

        if (urec->Blkprev() != INVALID_UNDO_REC_PTR) {
            xlUndoHeaderFlag |= UBTREE_XLOG_HAS_BLK_PREV;
        }
        if (urec->Prevurp2() != INVALID_UNDO_REC_PTR) {
            xlUndoHeaderFlag |= UBTREE_XLOG_HAS_XACT_PREV;
        }
        if (RelationIsPartition(rel)) {
            xlUndoHeaderFlag |= UBTREE_XLOG_HAS_PARTITION_OID;
        }

        xlrec.curXid = fxid;
        xlrec.prevXidOfTuple = t_thrd.ustore_cxt.undo_records[0]->OldXactId();
        xlrec.offNum = offset;
        xlrec.tdId = tdslot;
        // undo xlog stuff
        xlundohdr.relOid = relOid;
        xlundohdr.urecptr = urecPtr;
        xlundohdr.flag = xlUndoHeaderFlag;
        UndoRecPtr blkprev = urec->Blkprev();
        UndoRecPtr prevurp2 = urec->Prevurp2();
        IndexTuple xlogIndexTuple = (IndexTuple)((char*)(urec->rawdata_.data) + SizeOfUBTreeUndoInfoData);

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, SizeOfUbtree3InsertOrDelete);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterData((char*)xlogIndexTuple, IndexTupleSize(xlogIndexTuple));
        CommitSeqNo curCSN = InvalidCommitSeqNo;
        LogCSN(&curCSN);

        XLogRegisterData((char *)&xlundohdr, SizeOfXLUndoHeader);
        XLogRegisterData((char*)undoInfo, SizeOfUBTreeUndoInfoData);
        if ((xlUndoHeaderFlag & UBTREE_XLOG_HAS_BLK_PREV) != 0) {
            Assert(blkprev != INVALID_UNDO_REC_PTR);
            XLogRegisterData((char *)&(blkprev), sizeof(UndoRecPtr));
        }
        if ((xlUndoHeaderFlag & UBTREE_XLOG_HAS_XACT_PREV) != 0) {
            XLogRegisterData((char *)&(prevurp2), sizeof(UndoRecPtr));
        }
        if ((xlUndoHeaderFlag & UBTREE_XLOG_HAS_PARTITION_OID) != 0) {
            XLogRegisterData((char *)&(RelationGetRelid(rel)), sizeof(Oid));
        }

        undo::LogUndoMeta(xlumPtr);

        /* filtering by origin on a row level is much more efficient */
        XLogIncludeOrigin();

        recptr = XLogInsert(RM_UBTREE3_ID, XLOG_UBTREE3_DELETE_PCR);
        PageSetLSN(page, recptr);
        SetUndoPageLSN(t_thrd.ustore_cxt.urecvec, recptr);
        undo::SetUndoMetaLSN(recptr);
    }
    undo::FinishUndoMeta(persistence);
    END_CRIT_SECTION();
    //UBTreePCRVerify(rel, page, BufferGetBlockNumber(buf), offset);         // TODO

    bool needRecordEmpty = (opaque->activeTupleCount == 0);
    if (needRecordEmpty) {
        /*
        * This delete operation deleted the last tuple in the page, This page is likely
        * to be empty later, so record this empty hint into a queue.
        */
        UBTreeRecordEmptyPage(rel, BufferGetBlockNumber(buf), opaque->last_delete_xid);
        /* release buffer */
        _bt_relbuf(rel, buf);
        /* at the same time, try to recycle an empty page already exists */
        UBTreePCRTryRecycleEmptyPage(rel);
    } else {
        /* just release buffer */
        _bt_relbuf(rel, buf);
    }
}

/*
 * UBTreePageIndexTupleDelete
 *
 * This routine does the work of removing a tuple from an index page.
 *
 * Unlike heap pages, we compact out the line pointer for the removed tuple.
 */
void UBTreePCRPageIndexTupleDelete(Page page, OffsetNumber offnum)
{
    PageHeader phdr = (PageHeader)page;
    char *addr = NULL;
    ItemId tup;
    Size size;
    unsigned offset;
    int nbytes;
    int offidx;
    int nline;
    errno_t rc = EOK;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    Assert(!PageIsCompressed(page));
    if (phdr->pd_lower < SizeOfPageHeaderData + SizeOfUBTreeTDData(page)|| phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                               phdr->pd_upper, phdr->pd_special)));

    nline = UBTreePCRPageGetMaxOffsetNumber(page);
    if ((int)offnum <= 0 || (int)offnum > nline)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                        errmsg("invalid index offnum: %u", offnum)));

    /* change offset number to offset index */
    offidx = offnum - 1;

    tup = UBTreePCRGetRowPtr(page, offnum);
    Assert(ItemIdHasStorage(tup));
    size = ItemIdGetLength(tup);
    offset = ItemIdGetOffset(tup);
    if (offset < phdr->pd_upper || (offset + size) > phdr->pd_special || offset != (unsigned int)(MAXALIGN(offset)) ||
        size != (unsigned int)(MAXALIGN(size)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted item pointer: offset = %u, size = %u", offset, (unsigned int)size)));

    /*
     * First, we want to get rid of the pd_linp entry for the index tuple. We
     * copy all subsequent linp's back one slot in the array. We don't use
     * PageGetItemId, because we are manipulating the _array_, not individual
     * linp's.
     */
    nbytes = phdr->pd_lower - ((char *)&phdr->pd_linp[0] - (char *)phdr + (offidx + 1) * sizeof(ItemIdData) +
        SizeOfUBTreeTDData(page));

    if (nbytes > 0) {
        rc = memmove_s((char *)&(phdr->pd_linp[0]) + offidx * sizeof(ItemIdData) + SizeOfUBTreeTDData(page),
            nbytes, (char *)&(phdr->pd_linp[0])+ (offidx + 1 ) * sizeof(ItemIdData) +
            SizeOfUBTreeTDData(page), nbytes);
        securec_check(rc, "", "");
    }

    /*
     * Now move everything between the old upper bound (beginning of tuple
     * space) and the beginning of the deleted tuple forward, so that space in
     * the middle of the page is left free.  If we've just deleted the tuple
     * at the beginning of tuple space, then there's no need to do the copy
     * (and bcopy on some architectures SEGV's if asked to move zero bytes).
     */
    /* beginning of tuple space */
    addr = (char *)page + phdr->pd_upper;

    if (offset > phdr->pd_upper) {
        rc = memmove_s(addr + size, (int)(offset - phdr->pd_upper), addr, (int)(offset - phdr->pd_upper));
        securec_check(rc, "", "");
    }

    /* adjust free space boundary pointers */
    phdr->pd_upper += size;
    phdr->pd_lower -= sizeof(ItemIdData);

    /*
     * Finally, we need to adjust the linp entries that remain.
     *
     * Anything that used to be before the deleted tuple's data was moved
     * forward by the size of the deleted tuple.
     */
    if (!PageIsEmpty(page)) {
        int i;

        nline--; /* there's one less than when we started */
        for (i = 1; i <= nline; i++) {
            ItemId ii = UBTreePCRGetRowPtr(phdr, i);

            Assert(ItemIdHasStorage(ii));
            if (ItemIdGetOffset(ii) <= offset)
                ii->lp_off += size;
        }
    }
}


/*
 * Check if index tuple have appropriate number of attributes.
 */
bool UBTreePCRCheckNatts(const Relation index, bool heapkeyspace, Page page, OffsetNumber offnum)
{
    int16 natts = IndexRelationGetNumberOfAttributes(index);
    int16 nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTuple itup;
    int tupnatts;

    /*
     * We cannot reliably test a deleted or half-dead page, since they have
     * dummy high keys
     */
    if (P_IGNORE(opaque)) {
        return true;
    }

    Assert(offnum >= FirstOffsetNumber && offnum <= UBTreePCRPageGetMaxOffsetNumber(page));

    itup = (IndexTuple) UBTreePCRGetIndexTuple(page, offnum);

    if (P_ISLEAF(opaque) && offnum >= P_FIRSTDATAKEY(opaque)) {
        /*
         * Regular leaf tuples have as every index attributes
         */
        return (UBTreeTupleGetNAtts(itup, index) == natts);
    } else if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque)) {
        /*
         * Leftmost tuples on non-leaf pages have no attributes, or haven't
         * INDEX_ALT_TID_MASK set in pg_upgraded indexes.
         */
        return (UBTreeTupleGetNAtts(itup, index) == 0 || ((itup->t_info & INDEX_ALT_TID_MASK) == 0));
    } else {
        /*
         * Pivot tuples stored in non-leaf pages and hikeys of leaf pages
         * contain only key attributes
         */
        if (!heapkeyspace) {
            return (UBTreeTupleGetNAtts(itup, index) <= nkeyatts);
        }
    }

    /* Handle heapkeyspace pivot tuples (excluding minus infinity items) */
    Assert(heapkeyspace);

    /*
     * Explicit representation of the number of attributes is mandatory with
     * heapkeyspace index pivot tuples, regardless of whether or not there are
     * non-key attributes.
     */
    if (!UBTreeTupleIsPivot(itup))
        return false;

    tupnatts = UBTreeTupleGetNAtts(itup, index);
    /*
     * Heap TID is a tiebreaker key attribute, so it cannot be untruncated
     * when any other key attribute is truncated
     */
    if (UBTreeTupleGetHeapTID(itup) != NULL && tupnatts != nkeyatts) {
        return false;
    }

    /*
     * Pivot tuple must have at least one untruncated key attribute (minus
     * infinity pivot tuples are the only exception).  Pivot tuples can never
     * represent that there is a value present for a key attribute that
     * exceeds pg_index.indnkeyatts for the index.
     */
    return tupnatts > 0 && tupnatts <= nkeyatts;
}

void VerifyPCRIndexHikeyAndOpaque(Relation rel, Page page, BlockNumber blkno)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    if (P_ISLEAF(opaque) ? (opaque->btpo.level != 0) : (opaque->btpo.level == 0)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted rel %s, level %u, flag %u, rnode[%u,%u,%u], block %u.",
            NameStr(rel->rd_rel->relname), opaque->btpo.level, opaque->btpo_flags,
            rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno)));
    }

    /* compare last key and HIKEY */
    OffsetNumber lastPos = UBTreePCRPageGetMaxOffsetNumber(page);
    /* note that the first data key of internal pages has no value */
    if (!P_RIGHTMOST(opaque) && (P_ISLEAF(opaque) ? (lastPos > P_HIKEY) : (lastPos > P_FIRSTKEY))) {
        IndexTuple lastTuple = (IndexTuple)UBTreePCRGetIndexTuple(page, lastPos);

        /* we must hold: hikey >= lastKey */
        BTScanInsert itupKey = UBTreeMakeScanKey(rel, lastTuple);
        if (UBTreePCRCompare(rel, itupKey, page, P_HIKEY) > 0) {
            Datum values[INDEX_MAX_KEYS];
            bool isnull[INDEX_MAX_KEYS];
            index_deform_tuple(lastTuple, RelationGetDescr(rel), values, isnull);
            char *keyDesc = BuildIndexValueDescription(rel, values, isnull);
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY corrupted key %s with HIKEY compare in rel %s, rnode[%u,%u,%u], block %u.",
                (keyDesc ? keyDesc : "(UNKNOWN)"), NameStr(rel->rd_rel->relname),
                rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno)));
        }
        pfree(itupKey);
    }
}

void VerifyUBTreePCRTD(Relation rel, Page page, BlockNumber blkno)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    uint16 tdCount = UBTreePageGetTDSlotCount(page);
    if (P_ISLEAF(opaque) ? (opaque->btpo.level != 0) : (opaque->btpo.level == 0)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted rel %s, level %u, flag %u, rnode[%u,%u,%u], block %u.",
            NameStr(rel->rd_rel->relname), opaque->btpo.level, opaque->btpo_flags,
            rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno)));
    }

    /* compare last key and HIKEY */
    OffsetNumber lastPos = UBTreePCRPageGetMaxOffsetNumber(page);
    /* note that the first data key of internal pages has no value */
    if (!P_RIGHTMOST(opaque) && (P_ISLEAF(opaque) ? (lastPos > P_HIKEY) : (lastPos > P_FIRSTKEY))) {
        IndexTuple lastTuple = (IndexTuple)UBTreePCRGetIndexTuple(page, lastPos);

        /* we must hold: hikey >= lastKey */
        BTScanInsert itupKey = UBTreeMakeScanKey(rel, lastTuple);
        if (UBTreePCRCompare(rel, itupKey, page, P_HIKEY) > 0) {
            Datum values[INDEX_MAX_KEYS];
            bool isnull[INDEX_MAX_KEYS];
            index_deform_tuple(lastTuple, RelationGetDescr(rel), values, isnull);
            char *keyDesc = BuildIndexValueDescription(rel, values, isnull);
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY corrupted key %s with HIKEY compare in rel %s, rnode[%u,%u,%u], block %u.",
                (keyDesc ? keyDesc : "(UNKNOWN)"), NameStr(rel->rd_rel->relname),
                rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno)));
        }
        pfree(itupKey);
    }
}

void UBTreePCRRecordGetNewPageCost(UBTreeGetNewPageStats* stats, NewPageCostType type, TimestampTz start)
{
    if (stats) {
        TimestampTz cost = GetCurrentTimestamp() - start;
        switch (type) {
            case GET_PAGE:
                stats->getAvailablePageTime += cost;
                stats->getAvailablePageCount++;
                stats->getAvailablePageTimeMax = Max(stats->getAvailablePageTimeMax, cost);
                break;
            case ADD_BLOCKS:
                stats->addExtraBlocksTime += cost;
                stats->addExtraBlocksCount++;
                stats->addExtraBlocksTimeMax = Max(stats->addExtraBlocksTimeMax, cost);
                break;
            case EXTEND_ONE:
                stats->extendOneTime += cost;
                stats->extendOneCount++;
                stats->extendOneTimeMax = Max(stats->extendOneTimeMax, cost);
                break;
            case URQ_GET_PAGE:
                stats->getOnUrqPageTime += cost;
                stats->getOnUrqPageCount++;
                stats->getOnUrqPageTimeMax = Max(stats->getOnUrqPageTimeMax, cost);
                break;
            default:
                break;
        }
    }
}

/*
 * Log the reuse of a page from the recycle queue.
 */
static void UBTreePCRLogReusePage(Relation rel, BlockNumber blkno, TransactionId latestRemovedXid)
{
    xl_btree_reuse_page xlrec;

    // if (!RelationNeedsWAL(rel))
    //     return;

    // /*
    //  * Note that we don't register the buffer with the record, because this
    //  * operation doesn't modify the page. This record only exists to provide a
    //  * conflict point for Hot Standby.
    //  *
    //  * XLOG stuff
    //  */
    // RelFileNodeRelCopy(xlrec.node, rel->rd_node);

    // xlrec.block = blkno;
    // xlrec.latestRemovedXid = latestRemovedXid;

    // XLogBeginInsert();
    // XLogRegisterData((char *)&xlrec, SizeOfBtreeReusePage);

    // (void)XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_REUSE_PAGE, rel->rd_node.bucketNode);
}

static void UBTreePCRLogAndFreeNewPageStats(UBTreeGetNewPageStats* stats)
{
    if (stats) {
        ereport(LOG, (errmodule(MOD_UBTREE), (errmsg(
            "UBTreeGetNewPageStats: rnode=[%u, %u, %u], getAvailablePage[total, count, max]=[%ld, %u, %ld], "
            "addExtraBlocks[total, count, max]=[%ld, %u, %ld], extendOne[total, count, max]=[%ld, %u, %ld], "
            "getOnUrqPage[total, count, max]=[%ld, %u, %ld],"
            "urqItemsCount:%u; restartCount first:%u, checkNonTrackedPagesCount:%u;",
            stats->spcnode, stats->dbnode, stats->relnode,
            stats->getAvailablePageTime, stats->getAvailablePageCount, stats->getAvailablePageTimeMax,
            stats->addExtraBlocksTime, stats->addExtraBlocksCount, stats->addExtraBlocksTimeMax,
            stats->extendOneTime, stats->extendOneCount, stats->extendOneTimeMax,
            stats->getOnUrqPageTime, stats->getOnUrqPageCount, stats->getOnUrqPageTimeMax,
            stats->urqItemsCount, stats->restartCount, stats->checkNonTrackedPagesCount))));
        pfree(stats);
    }
}


/*
 *	UBTreePCRGetNewPage() -- Allocate a new page.
 *
 *		This routine will allocate a new page from Recycle Queue or extend the
 *		relation.
 *
 *		We will try to found a free page from the freed fork of Recycle Queue, and
 *		extend the relation when there is no free page in Recycle Queue.
 *
 *		addr is a output parameter, it will be set to a valid value if the page
 *		is found from Recycle Queue. This output tells where is the corresponding
 *		page in the Recycle Queue, and we need to call UBTreeRecordUsedPage()
 *		with this addr when the returned page is used correctly.
 */
Buffer UBTreePCRGetNewPage(Relation rel, UBTRecycleQueueAddress* addr)
{
    WHITEBOX_TEST_STUB("UBTreePCRGetNewPage-begin", WhiteboxDefaultErrorEmit);
    TimestampTz startTime = 0;
    UBTreeGetNewPageStats* stats = NULL;
    if (module_logging_is_on(MOD_UBTREE)) {
        stats = (UBTreeGetNewPageStats*)palloc0(sizeof(UBTreeGetNewPageStats));
        stats->spcnode = rel->rd_node.spcNode;
        stats->dbnode = rel->rd_node.dbNode;
        stats->relnode = rel->rd_node.relNode;
    }
restart:
    if (stats) {
        stats->restartCount++;
        startTime = GetCurrentTimestamp();
    }
    Buffer buf = UBTreePCRGetAvailablePage(rel, RECYCLE_FREED_FORK, addr, stats);
    UBTreePCRRecordGetNewPageCost(stats, GET_PAGE, startTime);
    if (buf == InvalidBuffer) {
        /*
         * No free page left, need to extend the relation
         *
         * Extend the relation by one page.
         *
         * We have to use a lock to ensure no one else is extending the rel at
         * the same time, else we will both try to initialize the same new
         * page.  We can skip locking for new or temp relations, however,
         * since no one else could be accessing them.
         */
        bool needLock = !RELATION_IS_LOCAL(rel);
        if (needLock) {
            if (!ConditionalLockRelationForExtension(rel, ExclusiveLock)) {
                /* couldn't get the lock immediately; wait for it. */
                LockRelationForExtension(rel, ExclusiveLock);
                if (stats) {
                    startTime = GetCurrentTimestamp();
                }
                /* check again, relation may extended by other backends */
                buf = UBTreePCRGetAvailablePage(rel, RECYCLE_FREED_FORK, addr, stats);
                UBTreePCRRecordGetNewPageCost(stats, GET_PAGE, startTime);
                if (buf != InvalidBuffer) {
                    UnlockRelationForExtension(rel, ExclusiveLock);
                    goto out;
                }
                if (stats) {
                    startTime = GetCurrentTimestamp();
                }
                /* Time to bulk-extend. */
                RelationAddExtraBlocks(rel, NULL);
                UBTreePCRRecordGetNewPageCost(stats, ADD_BLOCKS, startTime);
                WHITEBOX_TEST_STUB("UBTreePCRGetNewPage-bulk-extend", WhiteboxDefaultErrorEmit);
            }
        }
        if (stats) {
            startTime = GetCurrentTimestamp();
        }
        /* extend by one page */
        buf = ReadBuffer(rel, P_NEW);
        UBTreePCRRecordGetNewPageCost(stats, EXTEND_ONE, startTime);
        WHITEBOX_TEST_STUB("UBTreePCRGetNewPage-extend", WhiteboxDefaultErrorEmit);
        if (!ConditionalLockBuffer(buf)) {
            /* lock failed. To avoid dead lock, we need to retry */
            if (needLock) {
                UnlockRelationForExtension(rel, ExclusiveLock);
            }
            ReleaseBuffer(buf);
            goto restart;
        }
        /*
         * Release the file-extension lock; it's now OK for someone else to
         * extend the relation some more.
         */
        if (needLock)
            UnlockRelationForExtension(rel, ExclusiveLock);

        /* we have successfully extended the space, get the new page and write lock */
        addr->queueBuf = InvalidBuffer; /* not allocated from recycle */
    }
out:
    /* buffer is valid, exclusive lock already acquired */
    Assert(BufferIsValid(buf));

    Page page = BufferGetPage(buf);
    if (!UBTreePCRPageRecyclable(page)) {
        /* oops, failure due to concurrency, retry. */
        UnlockReleaseBuffer(buf);
        if (BufferIsValid(addr->queueBuf)) {
            ReleaseBuffer(addr->queueBuf);
            addr->queueBuf = InvalidBuffer;
        }
        goto restart;
    }

    if (addr->queueBuf != InvalidBuffer) {
        /*
         * If we are generating WAL for Hot Standby then create a
         * WAL record that will allow us to conflict with queries
         * running on standby.
         */
        if (XLogStandbyInfoActive() && RelationNeedsWAL(rel)) {
            UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            UBTreePCRLogReusePage(rel, BufferGetBlockNumber(buf), opaque->last_delete_xid);
        }
    }
    UBTreePCRPageInit(page, BufferGetPageSize(buf));
    UBTreePCRLogAndFreeNewPageStats(stats);
    return buf;
}
