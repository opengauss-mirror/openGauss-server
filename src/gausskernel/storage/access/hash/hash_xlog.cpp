/* -------------------------------------------------------------------------
 *
 * hash_xlog.cpp
 *    WAL replay logic for hash index.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/hash/hash_xlog.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/xlogproc.h"
#include "access/hash.h"
#include "access/hash_xlog.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "access/transam.h"
#include "access/xlogproc.h"
#include "storage/procarray.h"
#include "miscadmin.h"

/*
 * replay a hash index meta page
 */
static void hash_xlog_init_meta_page(XLogReaderState *record)
{
    RedoBufferInfo metabuf;
    ForkNumber forknum;

    /* create the index' metapage */
    XLogInitBufferForRedo(record, 0, &metabuf);
    Assert(BufferIsValid(metabuf.buf));
    HashRedoInitMetaPageOperatorPage(&metabuf, XLogRecGetData(record));
    MarkBufferDirty(metabuf.buf);

    /*
     * Force the on-disk state of init forks to always be in sync with the
     * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
     * special handling for init forks as create index operations don't log a
     * full page image of the metapage.
     */
    XLogRecGetBlockTag(record, 0, NULL, &forknum, NULL);
    if (forknum == INIT_FORKNUM)
        FlushOneBuffer(metabuf.buf);

    /* all done */
    UnlockReleaseBuffer(metabuf.buf);
}

/*
 * replay a hash index bitmap page
 */
static void hash_xlog_init_bitmap_page(XLogReaderState *record)
{
    RedoBufferInfo bitmapbuf;
    RedoBufferInfo metabuf;
    ForkNumber forknum;

    /*
     * Initialize bitmap page
     */
    XLogInitBufferForRedo(record, 0, &bitmapbuf);
    HashRedoInitBitmapPageOperatorBitmapPage(&bitmapbuf, XLogRecGetData(record));
    MarkBufferDirty(bitmapbuf.buf);

    /*
     * Force the on-disk state of init forks to always be in sync with the
     * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
     * special handling for init forks as create index operations don't log a
     * full page image of the metapage.
     */
    XLogRecGetBlockTag(record, 0, NULL, &forknum, NULL);
    if (forknum == INIT_FORKNUM)
        FlushOneBuffer(bitmapbuf.buf);
    UnlockReleaseBuffer(bitmapbuf.buf);

    /* add the new bitmap page to the metapage's list of bitmaps */
    if (XLogReadBufferForRedo(record, 1, &metabuf) == BLK_NEEDS_REDO) {
        /*
         * Note: in normal operation, we'd update the metapage while still
         * holding lock on the bitmap page.  But during replay it's not
         * necessary to hold that lock, since nobody can see it yet; the
         * creating transaction hasn't yet committed.
         */
        HashRedoInitBitmapPageOperatorMetaPage(&metabuf);
        MarkBufferDirty(metabuf.buf);

        XLogRecGetBlockTag(record, 1, NULL, &forknum, NULL);
        if (forknum == INIT_FORKNUM)
            FlushOneBuffer(metabuf.buf);
    }
    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

/*
 * replay a hash index insert without split
 */
static void hash_xlog_insert(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    RedoBufferInfo metabuf;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        Size datalen;
        char *datapos = XLogRecGetBlockData(record, 0, &datalen);

        HashRedoInsertOperatorPage(&buffer, XLogRecGetData(record), datapos, datalen);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    if (XLogReadBufferForRedo(record, 1, &metabuf) == BLK_NEEDS_REDO) {
        /*
         * Note: in normal operation, we'd update the metapage while still
         * holding lock on the page we inserted into.  But during replay it's
         * not necessary to hold that lock, since no other index updates can
         * be happening concurrently.
         */
        HashRedoInsertOperatorMetaPage(&metabuf);
        MarkBufferDirty(metabuf.buf);
    }
    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

/*
 * replay addition of overflow page for hash index
 */
static void hash_xlog_add_ovfl_page(XLogReaderState* record)
{
    RedoBufferInfo leftbuf;
    RedoBufferInfo ovflbuf;
    RedoBufferInfo metabuf;
    BlockNumber leftblk;
    BlockNumber rightblk;
    char *data = NULL;
    Size datalen;

    XLogRecGetBlockTag(record, 0, NULL, NULL, &rightblk);
    XLogRecGetBlockTag(record, 1, NULL, NULL, &leftblk);

    XLogInitBufferForRedo(record, 0, &ovflbuf);
    Assert(BufferIsValid(ovflbuf.buf));

    data = XLogRecGetBlockData(record, 0, &datalen);
    HashRedoAddOvflPageOperatorOvflPage(&ovflbuf, leftblk, data, datalen);
    MarkBufferDirty(ovflbuf.buf);

    if (XLogReadBufferForRedo(record, 1, &leftbuf) == BLK_NEEDS_REDO) {
        HashRedoAddOvflPageOperatorLeftPage(&leftbuf, rightblk);
        MarkBufferDirty(leftbuf.buf);
    }

    if (BufferIsValid(leftbuf.buf))
        UnlockReleaseBuffer(leftbuf.buf);
    UnlockReleaseBuffer(ovflbuf.buf);

    /*
     * Note: in normal operation, we'd update the bitmap and meta page while
     * still holding lock on the overflow pages.  But during replay it's not
     * necessary to hold those locks, since no other index updates can be
     * happening concurrently.
     */
    if (XLogRecHasBlockRef(record, 2)) {
        RedoBufferInfo mapbuffer;

        if (XLogReadBufferForRedo(record, 2, &mapbuffer) == BLK_NEEDS_REDO) {
            data = XLogRecGetBlockData(record, 2, &datalen);

            HashRedoAddOvflPageOperatorMapPage(&mapbuffer, data);
            MarkBufferDirty(mapbuffer.buf);
        }
        if (BufferIsValid(mapbuffer.buf))
            UnlockReleaseBuffer(mapbuffer.buf);
    }

    if (XLogRecHasBlockRef(record, 3)) {
        RedoBufferInfo newmapbuf;

        XLogInitBufferForRedo(record, 3, &newmapbuf);

        HashRedoAddOvflPageOperatorNewmapPage(&newmapbuf, XLogRecGetData(record));
        MarkBufferDirty(newmapbuf.buf);

        UnlockReleaseBuffer(newmapbuf.buf);
    }

    if (XLogReadBufferForRedo(record, 4, &metabuf) == BLK_NEEDS_REDO) {
        data = XLogRecGetBlockData(record, 4, &datalen);

        HashRedoAddOvflPageOperatorMetaPage(&metabuf, XLogRecGetData(record), data, datalen);
        MarkBufferDirty(metabuf.buf);
    }
    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

/*
 * replay allocation of page for split operation
 */
static void hash_xlog_split_allocate_page(XLogReaderState *record)
{
    RedoBufferInfo oldbuf;
    RedoBufferInfo newbuf;
    RedoBufferInfo metabuf;
    Size datalen PG_USED_FOR_ASSERTS_ONLY;
    char *data = NULL;
    XLogRedoAction action;

    /*
     * To be consistent with normal operation, here we take cleanup locks on
     * both the old and new buckets even though there can't be any concurrent
     * inserts.
     */

    /* replay the record for old bucket */
    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &oldbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the special space is not included in the image.
     */
    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
        HashRedoSplitAllocatePageOperatorObukPage(&oldbuf, XLogRecGetData(record));
        MarkBufferDirty(oldbuf.buf);
    }

    /* replay the record for new bucket */
    XLogInitBufferForRedo(record, 1, &newbuf);
    HashRedoSplitAllocatePageOperatorNbukPage(&newbuf, XLogRecGetData(record));
    if (!IsBufferCleanupOK(newbuf.buf))
        elog(PANIC, "hash_xlog_split_allocate_page: failed to acquire cleanup lock");
    MarkBufferDirty(newbuf.buf);

    /*
     * We can release the lock on old bucket early as well but doing here to
     * consistent with normal operation.
     */
    if (BufferIsValid(oldbuf.buf))
        UnlockReleaseBuffer(oldbuf.buf);
    if (BufferIsValid(newbuf.buf))
        UnlockReleaseBuffer(newbuf.buf);

    /*
     * Note: in normal operation, we'd update the meta page while still
     * holding lock on the old and new bucket pages.  But during replay it's
     * not necessary to hold those locks, since no other bucket splits can be
     * happening concurrently.
     */

    /* replay the record for metapage changes */
    if (XLogReadBufferForRedo(record, 2, &metabuf) == BLK_NEEDS_REDO) {
        data = XLogRecGetBlockData(record, 2, &datalen);

        HashRedoSplitAllocatePageOperatorMetaPage(&metabuf, XLogRecGetData(record), data);
        MarkBufferDirty(metabuf.buf);
    }

    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

/*
 * replay of split operation
 */
static void hash_xlog_split_page(XLogReaderState *record)
{
    RedoBufferInfo buf;

    if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
        elog(ERROR, "Hash split record did not contain a full-page image");

    if (BufferIsValid(buf.buf))
        UnlockReleaseBuffer(buf.buf);
}

/*
 * replay completion of split operation
 */
static void hash_xlog_split_complete(XLogReaderState *record)
{
    RedoBufferInfo oldbuf;
    RedoBufferInfo newbuf;
    XLogRedoAction action;

    /* replay the record for old bucket */
    action = XLogReadBufferForRedo(record, 0, &oldbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the bucket flag is not included in the image.
     */
    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
        HashRedoSplitCompleteOperatorObukPage(&oldbuf, XLogRecGetData(record));
        MarkBufferDirty(oldbuf.buf);
    }
    if (BufferIsValid(oldbuf.buf))
        UnlockReleaseBuffer(oldbuf.buf);

    /* replay the record for new bucket */
    action = XLogReadBufferForRedo(record, 1, &newbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the bucket flag is not included in the image.
     */
    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
        HashRedoSplitCompleteOperatorNbukPage(&newbuf, XLogRecGetData(record));
        MarkBufferDirty(newbuf.buf);
    }
    if (BufferIsValid(newbuf.buf))
        UnlockReleaseBuffer(newbuf.buf);
}

/*
 * replay move of page contents for squeeze operation of hash index
 */
static void hash_xlog_move_page_contents(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_move_page_contents *xldata = (xl_hash_move_page_contents *) XLogRecGetData(record);
    RedoBufferInfo bucketbuf;
    RedoBufferInfo writebuf;
    RedoBufferInfo deletebuf;
    XLogRedoAction action;

    bucketbuf.buf = InvalidBuffer;
    writebuf.buf = InvalidBuffer;
    deletebuf.buf = InvalidBuffer;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (xldata->is_prim_bucket_same_wrt) {
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &writebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);

        PageSetLSN(bucketbuf.pageinfo.page, lsn);

        action = XLogReadBufferForRedo(record, 1, &writebuf);
    }

    /* replay the record for adding entries in overflow buffer */
    if (action == BLK_NEEDS_REDO) {
        char *data = NULL;
        Size datalen;

        data = XLogRecGetBlockData(record, 1, &datalen);

        HashXlogMoveAddPageOperatorPage(&writebuf, XLogRecGetData(record), (void *)data, datalen);

        MarkBufferDirty(writebuf.buf);
    }

    /* replay the record for deleting entries from overflow buffer */
    if (XLogReadBufferForRedo(record, 2, &deletebuf) == BLK_NEEDS_REDO) {
        char *ptr = NULL;
        Size len;

        ptr = XLogRecGetBlockData(record, 2, &len);

        HashXlogMoveDeleteOvflPageOperatorPage(&deletebuf, (void *)ptr, len);

        MarkBufferDirty(deletebuf.buf);
    }

    /*
     * Replay is complete, now we can release the buffers. We release locks at
     * end of replay operation to ensure that we hold lock on primary bucket
     * page till end of operation.  We can optimize by releasing the lock on
     * write buffer as soon as the operation for same is complete, if it is
     * not same as primary bucket page, but that doesn't seem to be worth
     * complicating the code.
     */
    if (BufferIsValid(deletebuf.buf))
        UnlockReleaseBuffer(deletebuf.buf);

    if (BufferIsValid(writebuf.buf))
        UnlockReleaseBuffer(writebuf.buf);

    if (BufferIsValid(bucketbuf.buf))
        UnlockReleaseBuffer(bucketbuf.buf);
}

/*
 * replay squeeze page operation of hash index
 */
static void hash_xlog_squeeze_page(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) XLogRecGetData(record);
    RedoBufferInfo bucketbuf;
    RedoBufferInfo writebuf;
    RedoBufferInfo ovflbuf;
    RedoBufferInfo prevbuf;
    RedoBufferInfo mapbuf;
    XLogRedoAction action;

    bucketbuf.buf = InvalidBuffer;
    prevbuf.buf = InvalidBuffer;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (xldata->is_prim_bucket_same_wrt) {
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &writebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);

        PageSetLSN(bucketbuf.pageinfo.page, lsn);

        action = XLogReadBufferForRedo(record, 1, &writebuf);
    }

    /* replay the record for adding entries in overflow buffer */
    if (action == BLK_NEEDS_REDO) {
        char *data = NULL;
        Size datalen;

        data = XLogRecGetBlockData(record, 1, &datalen);

        HashXlogSqueezeAddPageOperatorPage(&writebuf, XLogRecGetData(record), (void *)data, datalen);

        MarkBufferDirty(writebuf.buf);
    }

    /* replay the record for initializing overflow buffer */
    if (XLogReadBufferForRedo(record, 2, &ovflbuf) == BLK_NEEDS_REDO) {
        HashXlogSqueezeInitOvflbufOperatorPage(&ovflbuf, XLogRecGetData(record));

        MarkBufferDirty(ovflbuf.buf);
    }
    if (BufferIsValid(ovflbuf.buf))
        UnlockReleaseBuffer(ovflbuf.buf);

    /* replay the record for page previous to the freed overflow page */
    if (!xldata->is_prev_bucket_same_wrt &&
        XLogReadBufferForRedo(record, 3, &prevbuf) == BLK_NEEDS_REDO) {
        HashXlogSqueezeUpdatePrevPageOperatorPage(&prevbuf, XLogRecGetData(record));

        MarkBufferDirty(prevbuf.buf);
    }
    if (BufferIsValid(prevbuf.buf))
        UnlockReleaseBuffer(prevbuf.buf);

    /* replay the record for page next to the freed overflow page */
    if (XLogRecHasBlockRef(record, 4)) {
        RedoBufferInfo nextbuf;

        if (XLogReadBufferForRedo(record, 4, &nextbuf) == BLK_NEEDS_REDO) {
            HashXlogSqueezeUpdateNextPageOperatorPage(&nextbuf, XLogRecGetData(record));

            MarkBufferDirty(nextbuf.buf);
        }
        if (BufferIsValid(nextbuf.buf))
            UnlockReleaseBuffer(nextbuf.buf);
    }

    if (BufferIsValid(writebuf.buf))
        UnlockReleaseBuffer(writebuf.buf);

    if (BufferIsValid(bucketbuf.buf))
        UnlockReleaseBuffer(bucketbuf.buf);

    /*
     * Note: in normal operation, we'd update the bitmap and meta page while
     * still holding lock on the primary bucket page and overflow pages.  But
     * during replay it's not necessary to hold those locks, since no other
     * index updates can be happening concurrently.
     */
    /* replay the record for bitmap page */
    if (XLogReadBufferForRedo(record, 5, &mapbuf) == BLK_NEEDS_REDO) {
        char *data = NULL;
        Size datalen;

        data = XLogRecGetBlockData(record, 5, &datalen);
        HashXlogSqueezeUpdateBitmapOperatorPage(&mapbuf, (void *)data);

        MarkBufferDirty(mapbuf.buf);
    }
    if (BufferIsValid(mapbuf.buf))
        UnlockReleaseBuffer(mapbuf.buf);

    /* replay the record for meta page */
    if (XLogRecHasBlockRef(record, 6)) {
        RedoBufferInfo metabuf;

        if (XLogReadBufferForRedo(record, 6, &metabuf) == BLK_NEEDS_REDO) {
            char *data = NULL;
            Size datalen;

            data = XLogRecGetBlockData(record, 6, &datalen);
            HashXlogSqueezeUpdateMateOperatorPage(&metabuf, (void *)data);

            MarkBufferDirty(metabuf.buf);
        }
        if (BufferIsValid(metabuf.buf))
            UnlockReleaseBuffer(metabuf.buf);
    }
}

/*
 * replay delete operation of hash index
 */
static void hash_xlog_delete(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_delete *xldata = (xl_hash_delete *) XLogRecGetData(record);
    RedoBufferInfo bucketbuf;
    RedoBufferInfo deletebuf;
    XLogRedoAction action;

    bucketbuf.buf = InvalidBuffer;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (xldata->is_primary_bucket_page) {
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &deletebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);

        PageSetLSN(bucketbuf.pageinfo.page, lsn);

        action = XLogReadBufferForRedo(record, 1, &deletebuf);
    }

    /* replay the record for deleting entries in bucket page */
    if (action == BLK_NEEDS_REDO) {
        char *ptr = NULL;
        Size len;

        ptr = XLogRecGetBlockData(record, 1, &len);

        HashXlogDeleteBlockOperatorPage(&deletebuf, XLogRecGetData(record), (void *)ptr, len);

        MarkBufferDirty(deletebuf.buf);
    }
    if (BufferIsValid(deletebuf.buf))
        UnlockReleaseBuffer(deletebuf.buf);

    if (BufferIsValid(bucketbuf.buf))
        UnlockReleaseBuffer(bucketbuf.buf);
}

/*
 * replay split cleanup flag operation for primary bucket page.
 */
static void hash_xlog_split_cleanup(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        HashXlogSplitCleanupOperatorPage(&buffer);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

/*
 * replay for update meta page
 */
static void hash_xlog_update_meta_page(XLogReaderState *record)
{
    RedoBufferInfo metabuf;

    if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO) {
        HashXlogUpdateMetaOperatorPage(&metabuf, XLogRecGetData(record));

        MarkBufferDirty(metabuf.buf);
    }
    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

/*
 * Get the latestRemovedXid from the heap pages pointed at by the index
 * tuples being deleted. See also btree_xlog_delete_get_latestRemovedXid,
 * on which this function is based.
 */
static TransactionId hash_xlog_vacuum_get_latestRemovedXid(XLogReaderState *record)
{
    xl_hash_vacuum_one_page *xlrec;
    OffsetNumber *unused = NULL;
    Buffer ibuffer;
    Buffer hbuffer;
    Page ipage;
    Page hpage;
    RelFileNode rnode;
    BlockNumber blkno;
    ItemId iitemid;
    ItemId hitemid;
    IndexTuple itup;
    BlockNumber hblkno;
    OffsetNumber hoffnum;
    TransactionId latestRemovedXid = InvalidTransactionId;
    int i;

    xlrec = (xl_hash_vacuum_one_page *) XLogRecGetData(record);

    /*
     * If there's nothing running on the standby we don't need to derive a
     * full latestRemovedXid value, so use a fast path out of here.  This
     * returns InvalidTransactionId, and so will conflict with all HS
     * transactions; but since we just worked out that that's zero people,
     * it's OK.
     *
     * XXX There is a race condition here, which is that a new backend might
     * start just after we look.  If so, it cannot need to conflict, but this
     * coding will result in throwing a conflict anyway.
     */
    if (CountDBBackends(InvalidOid) == 0)
        return latestRemovedXid;

    /*
     * Check if WAL replay has reached a consistent database state. If not, we
     * must PANIC. See the definition of
     * btree_xlog_delete_get_latestRemovedXid for more details.
     */
    if (!t_thrd.xlog_cxt.reachedConsistency)
        elog(PANIC, "hash_xlog_vacuum_get_latestRemovedXid: cannot operate with inconsistent data");

    /*
     * Get index page.  If the DB is consistent, this should not fail, nor
     * should any of the heap page fetches below.  If one does, we return
     * InvalidTransactionId to cancel all HS transactions.  That's probably
     * overkill, but it's safe, and certainly better than panicking here.
     */
    XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);
    ibuffer = XLogReadBufferExtended(rnode, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);

    if (!BufferIsValid(ibuffer))
        return InvalidTransactionId;
    LockBuffer(ibuffer, HASH_READ);
    ipage = (Page) BufferGetPage(ibuffer);

    /*
     * Loop through the deleted index items to obtain the TransactionId from
     * the heap items they point to.
     */
    unused = (OffsetNumber *) ((char *) xlrec + SizeOfHashVacuumOnePage);

    for (i = 0; i < xlrec->ntuples; i++) {
        /*
         * Identify the index tuple about to be deleted.
         */
        iitemid = PageGetItemId(ipage, unused[i]);
        itup = (IndexTuple) PageGetItem(ipage, iitemid);

        /*
         * Locate the heap page that the index tuple points at
         */
        hblkno = ItemPointerGetBlockNumber(&(itup->t_tid));
        hbuffer = XLogReadBufferExtended(xlrec->hnode, MAIN_FORKNUM, hblkno, RBM_NORMAL, NULL);

        if (!BufferIsValid(hbuffer)) {
            UnlockReleaseBuffer(ibuffer);
            return InvalidTransactionId;
        }
        LockBuffer(hbuffer, HASH_READ);
        hpage = (Page) BufferGetPage(hbuffer);

        /*
         * Look up the heap tuple header that the index tuple points at by
         * using the heap node supplied with the xlrec. We can't use
         * heap_fetch, since it uses ReadBuffer rather than XLogReadBuffer.
         * Note that we are not looking at tuple data here, just headers.
         */
        hoffnum = ItemPointerGetOffsetNumber(&(itup->t_tid));
        hitemid = PageGetItemId(hpage, hoffnum);

        /*
         * Follow any redirections until we find something useful.
         */
        while (ItemIdIsRedirected(hitemid)) {
            hoffnum = ItemIdGetRedirect(hitemid);
            hitemid = PageGetItemId(hpage, hoffnum);
            CHECK_FOR_INTERRUPTS();
        }

        /*
         * If the heap item has storage, then read the header and use that to
         * set latestRemovedXid.
         *
         * Some LP_DEAD items may not be accessible, so we ignore them.
         */
        if (ItemIdHasStorage(hitemid)) {
            HeapTupleData tuple;
            tuple.t_data = (HeapTupleHeader) PageGetItem(hpage, hitemid);
            HeapTupleCopyBaseFromPage(&tuple, &hpage);
            HeapTupleHeaderAdvanceLatestRemovedXid(&tuple, &latestRemovedXid);
        } else if (ItemIdIsDead(hitemid)) {
            /*
             * Conjecture: if hitemid is dead then it had xids before the xids
             * marked on LP_NORMAL items. So we just ignore this item and move
             * onto the next, for the purposes of calculating
             * latestRemovedxids.
             */
        } else
            Assert(!ItemIdIsUsed(hitemid));

        UnlockReleaseBuffer(hbuffer);
    }

    UnlockReleaseBuffer(ibuffer);

    /*
     * If all heap tuples were LP_DEAD then we will be returning
     * InvalidTransactionId here, which avoids conflicts. This matches
     * existing logic which assumes that LP_DEAD tuples must already be older
     * than the latestRemovedXid on the cleanup record that set them as
     * LP_DEAD, hence must already have generated a conflict.
     */
    return latestRemovedXid;
}

/*
 * replay delete operation in hash index to remove
 * tuples marked as DEAD during index tuple insertion.
 */
static void hash_xlog_vacuum_one_page(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    RedoBufferInfo metabuf;
    XLogRedoAction action;

    /*
     * If we have any conflict processing to do, it must happen before we
     * update the page.
     *
     * Hash index records that are marked as LP_DEAD and being removed during
     * hash index tuple insertion can conflict with standby queries. You might
     * think that vacuum records would conflict as well, but we've handled
     * that already.  XLOG_HEAP2_CLEANUP_INFO records provide the highest xid
     * cleaned by the vacuum of the heap and so we can resolve any conflicts
     * just once when that arrives.  After that we know that no conflicts
     * exist from individual hash index vacuum records on that index.
     */
    if (InHotStandby) {
        TransactionId latestRemovedXid = hash_xlog_vacuum_get_latestRemovedXid(record);
        RelFileNode rnode;

        XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
        ResolveRecoveryConflictWithSnapshot(latestRemovedXid, rnode);
    }

    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &buffer);

    if (action == BLK_NEEDS_REDO) {
        Size len;

        len = XLogRecGetDataLen(record);
        HashXlogVacuumOnePageOperatorPage(&buffer, XLogRecGetData(record), len);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    if (XLogReadBufferForRedo(record, 1, &metabuf) == BLK_NEEDS_REDO) {
        HashXlogVacuumMateOperatorPage(&metabuf, XLogRecGetData(record));
        MarkBufferDirty(metabuf.buf);
    }
    if (BufferIsValid(metabuf.buf))
        UnlockReleaseBuffer(metabuf.buf);
}

void hash_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_HASH_INIT_META_PAGE:
            hash_xlog_init_meta_page(record);
            break;
        case XLOG_HASH_INIT_BITMAP_PAGE:
            hash_xlog_init_bitmap_page(record);
            break;
        case XLOG_HASH_INSERT:
            hash_xlog_insert(record);
            break;
        case XLOG_HASH_ADD_OVFL_PAGE:
            hash_xlog_add_ovfl_page(record);
            break;
        case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
            hash_xlog_split_allocate_page(record);
            break;
        case XLOG_HASH_SPLIT_PAGE:
            hash_xlog_split_page(record);
            break;
        case XLOG_HASH_SPLIT_COMPLETE:
            hash_xlog_split_complete(record);
            break;
        case XLOG_HASH_MOVE_PAGE_CONTENTS:
            hash_xlog_move_page_contents(record);
            break;
        case XLOG_HASH_SQUEEZE_PAGE:
            hash_xlog_squeeze_page(record);
            break;
        case XLOG_HASH_DELETE:
            hash_xlog_delete(record);
            break;
        case XLOG_HASH_SPLIT_CLEANUP:
            hash_xlog_split_cleanup(record);
            break;
        case XLOG_HASH_UPDATE_META_PAGE:
            hash_xlog_update_meta_page(record);
            break;
        case XLOG_HASH_VACUUM_ONE_PAGE:
            hash_xlog_vacuum_one_page(record);
            break;
        default:
            elog(PANIC, "hash_redo: unknown op code %u", info);
    }
}

bool IsHashVacuumPages(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (XLogRecGetRmid(record) == RM_HASH_ID) {
        if (info == XLOG_HASH_DELETE) {
            return true;
        }
    }

    return false;
}
