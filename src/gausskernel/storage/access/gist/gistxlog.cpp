/* -------------------------------------------------------------------------
 *
 * gistxlog.cpp
 *	  WAL replay logic for GiST.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/gist/gistxlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gist_private.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "utils/memutils.h"

/*
 * Replay the clearing of F_FOLLOW_RIGHT flag on a child page.
 *
 * Even if the WAL record includes a full-page image, we have to update the
 * follow-right flag, because that change is not included in the full-page
 * image.  To be sure that the intermediate state with the wrong flag value is
 * not visible to concurrent Hot Standby queries, this function handles
 * restoring the full-page image as well as updating the flag.  (Note that
 * we never need to do anything else to the child page in the current WAL
 * action.)
 */
static void gistRedoClearFollowRight(XLogReaderState *record, uint8 block_id)
{
    RedoBufferInfo buffer;
    XLogRedoAction action;

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the updated NSN is not included in the image.
     */
    action = XLogReadBufferForRedo(record, block_id, &buffer);
    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
        GistRedoClearFollowRightOperatorPage(&buffer);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

/*
 * redo any page update (except page split)
 */
static void gistRedoPageUpdateRecord(XLogReaderState *record)
{
    void *xldata = (void *)XLogRecGetData(record);
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        Size datalen;

        char *data = XLogRecGetBlockData(record, 0, &datalen);

        /* Delete old tuples */

        GistRedoPageUpdateOperatorPage(&buffer, xldata, data, datalen);

        /* add tuples */

        /*
         * special case: leafpage, nothing to insert, nothing to delete, then
         * vacuum marks page
         */

        /*
         * all links on non-leaf root page was deleted by vacuum full, so root
         * page becomes a leaf
         */

        MarkBufferDirty(buffer.buf);
    }

    /*
     * Fix follow-right data on left child page
     *
     * This must be done while still holding the lock on the target page. Note
     * that even if the target page no longer exists, we still attempt to
     * replay the change on the child page.
     */
    if (XLogRecHasBlockRef(record, 1))
        gistRedoClearFollowRight(record, 1);

    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

/*
 * Returns an array of index pointers.
 */
IndexTuple *decodePageSplitRecord(char *begin, int len, int *n)
{
    char *ptr = NULL;
    int i = 0;
    IndexTuple *tuples = NULL;
    errno_t ret = EOK;

    /* extract the number of tuples */
    ret = memcpy_s(n, sizeof(int), begin, sizeof(int));
    securec_check(ret, "", "");
    ptr = begin + sizeof(int);

    tuples = (IndexTuple *)palloc(*n * sizeof(IndexTuple));

    for (i = 0; i < *n; i++) {
        Assert(ptr - begin < len);
        tuples[i] = (IndexTuple)ptr;
        ptr += IndexTupleSize((IndexTuple)ptr);
    }
    Assert(ptr - begin == len);

    return tuples;
}

bool IsGistPageUpdate(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if ((XLogRecGetRmid(record) == RM_GIST_ID) && (info == XLOG_GIST_PAGE_SPLIT)) {
        return true;
    }
    return false;
}

static void gistRedoPageSplitRecord(XLogReaderState *record)
{
    gistxlogPageSplit *xldata = (gistxlogPageSplit *)XLogRecGetData(record);
    RedoBufferInfo firstbuffer;
    RedoBufferInfo buffer;
    int i;
    bool isrootsplit = false;
    firstbuffer.buf = InvalidBuffer;
    /*
     * We must hold lock on the first-listed page throughout the action,
     * including while updating the left child page (if any).  We can unlock
     * remaining pages in the list as soon as they've been written, because
     * there is no path for concurrent queries to reach those pages without
     * first visiting the first-listed page.
     *
     * loop around all pages
     */
    for (i = 0; i < xldata->npage; i++) {
        char *data = NULL;
        Size datalen;
        BlockNumber blkno;
        BlockNumber nextblkno = InvalidBlockNumber;
        bool markflag = false;

        XLogRecGetBlockTag(record, i + 1, NULL, NULL, &blkno);
        if (blkno == GIST_ROOT_BLKNO) {
            Assert(i == 0);
            isrootsplit = true;
        }

        XLogInitBufferForRedo(record, i + 1, &buffer);
        data = XLogRecGetBlockData(record, i + 1, &datalen);

        if (blkno != GIST_ROOT_BLKNO) {
            if (i < xldata->npage - 1) {
                XLogRecGetBlockTag(record, i + 2, NULL, NULL, &nextblkno);
            } else
                nextblkno = xldata->origrlink;
            ;
            if (i < xldata->npage - 1 && !isrootsplit && xldata->markfollowright)
                markflag = true;
        }
        GistRedoPageSplitOperatorPage(&buffer, (void *)xldata, data, datalen, markflag, nextblkno);

        MarkBufferDirty(buffer.buf);

        if (i == 0)
            firstbuffer = buffer;
        else
            UnlockReleaseBuffer(buffer.buf);
    }

    /* Fix follow-right data on left child page, if any */
    if (XLogRecHasBlockRef(record, 0))
        gistRedoClearFollowRight(record, 0);

    /* Finally, release lock on the first page */
    if (firstbuffer.buf != InvalidBuffer)
        UnlockReleaseBuffer(firstbuffer.buf);
}

static void gistRedoCreateIndex(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    XLogInitBufferForRedo(record, GIST_ROOT_BLKNO, &buffer);
    GistRedoCreateIndexOperatorPage(&buffer);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);
}

void gist_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    MemoryContext oldCxt;

    /*
     * GiST indexes do not require any conflict processing. NB: If we ever
     * implement a similar optimization we have in b-tree, and remove killed
     * tuples outside VACUUM, we'll need to handle that here.
     */
    oldCxt = MemoryContextSwitchTo(t_thrd.xlog_cxt.gist_opCtx);
    switch (info) {
        case XLOG_GIST_PAGE_UPDATE:
            gistRedoPageUpdateRecord(record);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            gistRedoPageSplitRecord(record);
            break;
        case XLOG_GIST_CREATE_INDEX:
            gistRedoCreateIndex(record);
            break;
        default:
            ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("gist_redo: unknown op code %hhu", info)));
    }

    MemoryContextSwitchTo(oldCxt);
    MemoryContextReset(t_thrd.xlog_cxt.gist_opCtx);
}

void gist_xlog_startup(void)
{
    t_thrd.xlog_cxt.gist_opCtx = createTempGistContext();
}

void gist_xlog_cleanup(void)
{
    if (t_thrd.xlog_cxt.gist_opCtx != NULL) {
        MemoryContextDelete(t_thrd.xlog_cxt.gist_opCtx);
        t_thrd.xlog_cxt.gist_opCtx = NULL;
    }
}

/*
 * Write WAL record of a page split.
 */
XLogRecPtr gistXLogSplit(bool page_is_leaf, SplitedPageLayout *dist, BlockNumber origrlink, GistNSN orignsn,
                         Buffer leftchildbuf, bool markfollowright)
{
    gistxlogPageSplit xlrec;
    SplitedPageLayout *ptr = NULL;
    int npage = 0;
    XLogRecPtr recptr;
    int i;

    for (ptr = dist; ptr; ptr = ptr->next)
        npage++;

    xlrec.origrlink = origrlink;
    xlrec.orignsn = orignsn;
    xlrec.origleaf = page_is_leaf;
    xlrec.npage = (uint16)npage;
    xlrec.markfollowright = markfollowright;

    XLogBeginInsert();

    /*
     * Include a full page image of the child buf. (only necessary if a
     * checkpoint happened since the child page was split)
     */
    if (BufferIsValid(leftchildbuf))
        XLogRegisterBuffer(0, leftchildbuf, REGBUF_STANDARD);

    /*
     * NOTE: We register a lot of data. The caller must've called
     * XLogEnsureRecordSpace() to prepare for that. We cannot do it here,
     * because we're already in a critical section. If you change the number
     * of buffer or data registrations here, make sure you modify the
     * XLogEnsureRecordSpace() calls accordingly!
     */
    XLogRegisterData((char *)&xlrec, sizeof(gistxlogPageSplit));

    i = 1;
    for (ptr = dist; ptr; ptr = ptr->next) {
        XLogRegisterBuffer(i, ptr->buffer, REGBUF_WILL_INIT);
        XLogRegisterBufData(i, (char *)&(ptr->block.num), sizeof(int));
        XLogRegisterBufData(i, (char *)ptr->list, ptr->lenlist);
        i++;
    }

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT);

    return recptr;
}

/*
 * Write XLOG record describing a page update. The update can include any
 * number of deletions and/or insertions of tuples on a single index page.
 *
 * If this update inserts a downlink for a split page, also record that
 * the F_FOLLOW_RIGHT flag on the child page is cleared and NSN set.
 *
 * Note that both the todelete array and the tuples are marked as belonging
 * to the target buffer; they need not be stored in XLOG if XLogInsert decides
 * to log the whole buffer contents instead.
 */
XLogRecPtr gistXLogUpdate(Buffer buffer, OffsetNumber *todelete, int ntodelete, IndexTuple *itup, int ituplen,
                          Buffer leftchildbuf)
{
    gistxlogPageUpdate xlrec;
    int i;
    XLogRecPtr recptr;

    xlrec.ntodelete = ntodelete;
    xlrec.ntoinsert = ituplen;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, sizeof(gistxlogPageUpdate));

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
    XLogRegisterBufData(0, (char *)todelete, sizeof(OffsetNumber) * ntodelete);

    /* new tuples */
    for (i = 0; i < ituplen; i++)
        XLogRegisterBufData(0, (char *)(itup[i]), IndexTupleSize(itup[i]));

    /*
     * Include a full page image of the child buf. (only necessary if a
     * checkpoint happened since the child page was split)
     */
    if (BufferIsValid(leftchildbuf))
        XLogRegisterBuffer(1, leftchildbuf, REGBUF_STANDARD);

    recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_UPDATE);

    return recptr;
}
