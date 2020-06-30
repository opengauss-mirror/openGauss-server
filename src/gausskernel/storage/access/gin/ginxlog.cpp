/* -------------------------------------------------------------------------
 *
 * ginxlog.cpp
 *	  WAL replay logic for inverted index.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/gausskernel/storage/access/gin/ginxlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gin_private.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "utils/memutils.h"

static void ginRedoClearIncompleteSplit(XLogReaderState* record, uint8 block_id)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO) {
        ginRedoClearIncompleteSplitOperatorPage(&buffer);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

static void ginRedoCreateIndex(XLogReaderState* record)
{
    RedoBufferInfo RootBuffer, MetaBuffer;

    XLogInitBufferForRedo(record, 0, &MetaBuffer);
    ginRedoCreateIndexOperatorMetaPage(&MetaBuffer);
    MarkBufferDirty(MetaBuffer.buf);

    XLogInitBufferForRedo(record, 1, &RootBuffer);
    ginRedoCreateIndexOperatorRootPage(&RootBuffer);
    MarkBufferDirty(RootBuffer.buf);

    UnlockReleaseBuffer(RootBuffer.buf);
    UnlockReleaseBuffer(MetaBuffer.buf);
}

static void ginRedoCreatePTree(XLogReaderState* record)
{
    RedoBufferInfo buffer;

    XLogInitBufferForRedo(record, 0, &buffer);

    ginRedoCreatePTreeOperatorPage(&buffer, XLogRecGetData(record));

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);
}

void ginRedoInsertEntry(RedoBufferInfo* buffer, bool isLeaf, BlockNumber rightblkno, void* rdata)
{
    Page page = buffer->pageinfo.page;
    ginxlogInsertEntry* data = (ginxlogInsertEntry*)rdata;
    OffsetNumber offset = data->offset;
    IndexTuple itup;

    Assert(!GinPageIsData(page));
    if (rightblkno != InvalidBlockNumber) {
        /* update link to right page after split */
        Assert(!GinPageIsLeaf(page));
        Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
        itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offset));
        GinSetDownlink(itup, rightblkno);
    }

    if (data->isDelete) {
        Assert(GinPageIsLeaf(page));
        Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
        PageIndexTupleDelete(page, offset);
    }

    itup = &data->tuple;

    if (PageAddItem(page, (Item)itup, IndexTupleSize(itup), offset, false, false) == InvalidOffsetNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add item to index page in %u/%u/%u",
                    buffer->blockinfo.rnode.spcNode,
                    buffer->blockinfo.rnode.dbNode,
                    buffer->blockinfo.rnode.relNode)));
    }
    PageSetLSN(page, buffer->lsn);
}

void ginRedoRecompress(Page page, ginxlogRecompressDataLeaf* data)
{
    int actionno;
    int segno;
    GinPostingList* oldseg = NULL;
    Pointer segmentend;
    char* walbuf = NULL;
    int totalsize;
    errno_t ret = EOK;

    /*
     * If the page is in pre-9.4 format, convert to new format first.
     */
    if (!GinPageIsCompressed(page)) {
        ItemPointer uncompressed = (ItemPointer)GinDataPageGetData(page);
        int nuncompressed = GinPageGetOpaque(page)->maxoff;
        int npacked;
        GinPostingList* plist = NULL;

        plist = ginCompressPostingList(uncompressed, nuncompressed, BLCKSZ, &npacked, false);
        Assert(npacked == nuncompressed);

        totalsize = SizeOfGinPostingList(plist);

        ret = memcpy_s(GinDataLeafPageGetPostingList(page),
            BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData)),
            plist,
            totalsize);
        securec_check(ret, "", "");
        GinDataPageSetDataSize(page, totalsize);
        GinPageSetCompressed(page);
        GinPageGetOpaque(page)->maxoff = InvalidOffsetNumber;
    }

    oldseg = GinDataLeafPageGetPostingList(page);
    segmentend = (Pointer)oldseg + GinDataLeafPageGetPostingListSize(page);
    segno = 0;

    walbuf = ((char*)data) + sizeof(ginxlogRecompressDataLeaf);
    for (actionno = 0; actionno < data->nactions; actionno++) {
        uint8 a_segno = *((uint8*)(walbuf++));
        uint8 a_action = *((uint8*)(walbuf++));
        GinPostingList* newseg = NULL;
        int newsegsize = 0;
        ItemPointerData* items = NULL;
        uint16 nitems = 0;
        ItemPointerData* olditems = NULL;
        int nolditems;
        ItemPointerData* newitems = NULL;
        int nnewitems;
        int segsize;
        Pointer segptr;
        int szleft;

        /* Extract all the information we need from the WAL record */
        if (a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE) {
            newseg = (GinPostingList*)walbuf;
            newsegsize = SizeOfGinPostingList(newseg);
            walbuf += SHORTALIGN((uint)newsegsize);
        }

        if (a_action == GIN_SEGMENT_ADDITEMS) {
            ret = memcpy_s(&nitems, sizeof(uint16), walbuf, sizeof(uint16));
            securec_check(ret, "", "");
            walbuf += sizeof(uint16);
            items = (ItemPointerData*)walbuf;
            walbuf += nitems * sizeof(ItemPointerData);
        }

        /* Skip to the segment that this action concerns */
        Assert(segno <= a_segno);
        while (segno < a_segno) {
            oldseg = GinNextPostingListSegment(oldseg);
            segno++;
        }

        /*
         * ADDITEMS action is handled like REPLACE, but the new segment to
         * replace the old one is reconstructed using the old segment from
         * disk and the new items from the WAL record.
         */
        if (a_action == GIN_SEGMENT_ADDITEMS) {
            int npacked;
            bool isColStore = (oldseg->type == ROW_STORE_TYPE) ? false : true;

            olditems = ginPostingListDecode(oldseg, &nolditems);

            newitems = ginMergeItemPointers(items, nitems, olditems, nolditems, &nnewitems);
            Assert(nnewitems == nolditems + nitems);

            newseg = ginCompressPostingList(newitems, nnewitems, BLCKSZ, &npacked, isColStore);
            Assert(npacked == nnewitems);

            newsegsize = SizeOfGinPostingList(newseg);
            a_action = GIN_SEGMENT_REPLACE;
        }

        segptr = (Pointer)oldseg;
        if (segptr != segmentend)
            segsize = SizeOfGinPostingList(oldseg);
        else {
            /*
             * Positioned after the last existing segment. Only INSERTs
             * expected here.
             */
            Assert(a_action == GIN_SEGMENT_INSERT);
            segsize = 0;
        }
        szleft = segmentend - segptr;

        switch (a_action) {
            case GIN_SEGMENT_DELETE:
                ret = memmove_s(segptr, BLCKSZ - (segptr - page), segptr + segsize, szleft - segsize);
                securec_check(ret, "", "");
                segmentend -= segsize;

                segno++;
                break;

            case GIN_SEGMENT_INSERT:
                /* make room for the new segment */
                ret = memmove_s(segptr + newsegsize, BLCKSZ - (segptr + newsegsize - page), segptr, szleft);
                securec_check(ret, "", "");
                /* copy the new segment in place */
                ret = memcpy_s(segptr, BLCKSZ - (segptr - page), newseg, newsegsize);
                securec_check(ret, "", "");
                segmentend += newsegsize;
                segptr += newsegsize;
                break;

            case GIN_SEGMENT_REPLACE:
                /* shift the segments that follow */
                ret = memmove_s(
                    segptr + newsegsize, BLCKSZ - (segptr + newsegsize - page), segptr + segsize, szleft - segsize);
                securec_check(ret, "", "");
                /* copy the replacement segment in place */
                ret = memcpy_s(segptr, BLCKSZ - (segptr - page), newseg, newsegsize);
                securec_check(ret, "", "");
                segmentend -= segsize;
                segmentend += newsegsize;
                segptr += newsegsize;
                segno++;
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
                        errmsg("unexpected GIN leaf action: %hhu", a_action)));
        }
        oldseg = (GinPostingList*)segptr;
    }

    totalsize = segmentend - (Pointer)GinDataLeafPageGetPostingList(page);
    GinDataPageSetDataSize(page, totalsize);
}

void ginRedoInsertData(RedoBufferInfo* buffer, bool isLeaf, BlockNumber rightblkno, void* rdata)
{
    Page page = buffer->pageinfo.page;

    Assert(GinPageIsData(page));
    if (isLeaf) {
        ginxlogRecompressDataLeaf* data = (ginxlogRecompressDataLeaf*)rdata;

        Assert(GinPageIsLeaf(page));

        ginRedoRecompress(page, data);
    } else {
        ginxlogInsertDataInternal* data = (ginxlogInsertDataInternal*)rdata;
        PostingItem* oldpitem = NULL;

        Assert(!GinPageIsLeaf(page));

        /* update link to right page after split */
        oldpitem = GinDataPageGetPostingItem(page, data->offset);
        PostingItemSetBlockNumber(oldpitem, rightblkno);

        GinDataPageAddPostingItem(page, &data->newitem, data->offset);
    }

    PageSetLSN(page, buffer->lsn);
}

static void ginRedoInsert(XLogReaderState* record)
{
    ginxlogInsert* data = (ginxlogInsert*)XLogRecGetData(record);
    RedoBufferInfo buffer;
#ifdef NOT_USED
    BlockNumber leftChildBlkno = InvalidBlockNumber;
#endif
    BlockNumber rightChildBlkno = InvalidBlockNumber;
    bool isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split.
     */
    if (!isLeaf) {
        char* payload = XLogRecGetData(record) + sizeof(ginxlogInsert);

#ifdef NOT_USED
        leftChildBlkno = BlockIdGetBlockNumber((BlockId)payload);
#endif
        payload += sizeof(BlockIdData);
        rightChildBlkno = BlockIdGetBlockNumber((BlockId)payload);

        ginRedoClearIncompleteSplit(record, 1);
    }

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        Size len;
        char* payload = XLogRecGetBlockData(record, 0, &len);

        /* How to insert the payload is tree-type specific */
        if (data->flags & GIN_INSERT_ISDATA) {
            ginRedoInsertData(&buffer, isLeaf, rightChildBlkno, payload);
        } else {
            ginRedoInsertEntry(&buffer, isLeaf, rightChildBlkno, payload);
        }

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

static void ginRedoSplit(XLogReaderState* record)
{
    ginxlogSplit* data = (ginxlogSplit*)XLogRecGetData(record);
    RedoBufferInfo lbuffer, rbuffer, rootbuf;
    bool isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
    bool isRoot = (data->flags & GIN_SPLIT_ROOT) != 0;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split
     */
    if (!isLeaf)
        ginRedoClearIncompleteSplit(record, 3);

    if (XLogReadBufferForRedo(record, 0, &lbuffer) != BLK_RESTORED)
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("GIN split record did not contain a full-page image of left page")));

    if (XLogReadBufferForRedo(record, 1, &rbuffer) != BLK_RESTORED)
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("GIN split record did not contain a full-page image of right page")));

    if (isRoot) {
        if (XLogReadBufferForRedo(record, 2, &rootbuf) != BLK_RESTORED)
            ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("GIN split record did not contain a full-page image of root page")));
        UnlockReleaseBuffer(rootbuf.buf);
    }

    UnlockReleaseBuffer(rbuffer.buf);
    UnlockReleaseBuffer(lbuffer.buf);
}

/*
 * VACUUM_PAGE record contains simply a full image of the page, similar to
 * an XLOG_FPI record.
 * This is functionally the same as heap_xlog_newpage.
 */
static void ginRedoVacuumPage(XLogReaderState* record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) != BLK_RESTORED) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("replay of gin entry tree page vacuum did not restore the page")));
    }
    UnlockReleaseBuffer(buffer.buf);
}

static void ginRedoVacuumDataLeafPage(XLogReaderState* record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        void* recorddata = (void*)XLogRecGetBlockData(record, 0, NULL);
        ginRedoVacuumDataOperatorLeafPage(&buffer, recorddata);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

static void ginRedoDeletePage(XLogReaderState* record)
{
    RedoBufferInfo dbuffer;
    RedoBufferInfo pbuffer;
    RedoBufferInfo lbuffer;

    if (XLogReadBufferForRedo(record, 0, &dbuffer) == BLK_NEEDS_REDO) {
        ginRedoDeletePageOperatorCurPage(&dbuffer);
        MarkBufferDirty(dbuffer.buf);
    }

    void* data = (void*)XLogRecGetData(record);
    if (XLogReadBufferForRedo(record, 1, &pbuffer) == BLK_NEEDS_REDO) {

        ginRedoDeletePageOperatorParentPage(&pbuffer, data);
        MarkBufferDirty(pbuffer.buf);
    }

    if (XLogReadBufferForRedo(record, 2, &lbuffer) == BLK_NEEDS_REDO) {
        ginRedoDeletePageOperatorLeftPage(&lbuffer, data);
        MarkBufferDirty(lbuffer.buf);
    }

    if (BufferIsValid(lbuffer.buf))
        UnlockReleaseBuffer(lbuffer.buf);
    if (BufferIsValid(pbuffer.buf))
        UnlockReleaseBuffer(pbuffer.buf);
    if (BufferIsValid(dbuffer.buf))
        UnlockReleaseBuffer(dbuffer.buf);
}

void ginRedoUpdateAddNewTail(RedoBufferInfo* buffer, BlockNumber newRightlink)
{
    Page page = buffer->pageinfo.page;

    GinPageGetOpaque(page)->rightlink = newRightlink;

    PageSetLSN(page, buffer->lsn);
}

static void ginRedoUpdateMetapage(XLogReaderState* record)
{
    ginxlogUpdateMeta* data = (ginxlogUpdateMeta*)XLogRecGetData(record);
    RedoBufferInfo metabuffer;
    RedoBufferInfo buffer;

    /*
     * Restore the metapage. This is essentially the same as a full-page
     * image, so restore the metapage unconditionally without looking at the
     * LSN, to avoid torn page hazards.
     */
    XLogInitBufferForRedo(record, 0, &metabuffer);

    ginRedoUpdateOperatorMetapage(&metabuffer, (void*)data);

    MarkBufferDirty(metabuffer.buf);

    if (data->ntuples > 0) {
        /*
         * insert into tail page
         */
        if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO) {
            Size totaltupsize;
            void* payload = (void*)XLogRecGetBlockData(record, 1, &totaltupsize);

            ginRedoUpdateOperatorTailPage(&buffer, payload, totaltupsize, data->ntuples);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf))
            UnlockReleaseBuffer(buffer.buf);
    } else if (data->prevTail != InvalidBlockNumber) {
        /*
         * New tail
         */
        if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO) {
            ginRedoUpdateAddNewTail(&buffer, data->newRightlink);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf))
            UnlockReleaseBuffer(buffer.buf);
    }

    UnlockReleaseBuffer(metabuffer.buf);
}

static void ginRedoInsertListPage(XLogReaderState* record)
{
    void* data = (void*)XLogRecGetData(record);
    RedoBufferInfo buffer;
    char* payload = NULL;
    Size totaltupsize;

    /* We always re-initialize the page. */
    XLogInitBufferForRedo(record, 0, &buffer);

    payload = XLogRecGetBlockData(record, 0, &totaltupsize);

    ginRedoInsertListPageOperatorPage(&buffer, data, payload, totaltupsize);
    MarkBufferDirty(buffer.buf);

    UnlockReleaseBuffer(buffer.buf);
}

bool IsGinVacuumPages(XLogReaderState* record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (XLogRecGetRmid(record) == RM_GIN_ID) {
        if ((info == XLOG_GIN_VACUUM_PAGE) || (info == XLOG_GIN_VACUUM_DATA_LEAF_PAGE) ||
            (info == XLOG_GIN_DELETE_PAGE) || (info == XLOG_GIN_DELETE_LISTPAGE)) {
            return true;
        }
    }

    return false;
}

static void ginRedoDeleteListPages(XLogReaderState* record)
{
    ginxlogDeleteListPages* data = (ginxlogDeleteListPages*)XLogRecGetData(record);
    RedoBufferInfo metabuffer;
    int i;

    XLogInitBufferForRedo(record, 0, &metabuffer);

    ginRedoDeleteListPagesOperatorPage(&metabuffer, (void*)&(data->metadata));

    MarkBufferDirty(metabuffer.buf);

    /*
     * In normal operation, shiftList() takes exclusive lock on all the
     * pages-to-be-deleted simultaneously.  During replay, however, it should
     * be all right to lock them one at a time.  This is dependent on the fact
     * that we are deleting pages from the head of the list, and that readers
     * share-lock the next page before releasing the one they are on. So we
     * cannot get past a reader that is on, or due to visit, any page we are
     * going to delete.  New incoming readers will block behind our metapage
     * lock and then see a fully updated page list.
     *
     * No full-page images are taken of the deleted pages. Instead, they are
     * re-initialized as empty, deleted pages. Their right-links don't need to
     * be preserved, because no new readers can see the pages, as explained
     * above.
     */
    for (i = 0; i < data->ndeleted; i++) {
        RedoBufferInfo buffer;

        XLogInitBufferForRedo(record, i + 1, &buffer);
        ginRedoDeleteListPagesMarkDelete(&buffer);
        MarkBufferDirty(buffer.buf);

        UnlockReleaseBuffer(buffer.buf);
    }
    UnlockReleaseBuffer(metabuffer.buf);
}

void gin_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    MemoryContext oldCtx;

    /*
     * GIN indexes do not require any conflict processing. NB: If we ever
     * implement a similar optimization as we have in b-tree, and remove
     * killed tuples outside VACUUM, we'll need to handle that here.
     */
    oldCtx = MemoryContextSwitchTo(t_thrd.xlog_cxt.gin_opCtx);
    switch (info) {
        case XLOG_GIN_CREATE_INDEX:
            ginRedoCreateIndex(record);
            break;
        case XLOG_GIN_CREATE_PTREE:
            ginRedoCreatePTree(record);
            break;
        case XLOG_GIN_INSERT:
            ginRedoInsert(record);
            break;
        case XLOG_GIN_SPLIT:
            ginRedoSplit(record);
            break;
        case XLOG_GIN_VACUUM_PAGE:
            ginRedoVacuumPage(record);
            break;
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            ginRedoVacuumDataLeafPage(record);
            break;
        case XLOG_GIN_DELETE_PAGE:
            ginRedoDeletePage(record);
            break;
        case XLOG_GIN_UPDATE_META_PAGE:
            ginRedoUpdateMetapage(record);
            break;
        case XLOG_GIN_INSERT_LISTPAGE:
            ginRedoInsertListPage(record);
            break;
        case XLOG_GIN_DELETE_LISTPAGE:
            ginRedoDeleteListPages(record);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("gin_redo: unknown op code %hhu", info)));
    }
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(t_thrd.xlog_cxt.gin_opCtx);
}

void gin_xlog_startup(void)
{
    t_thrd.xlog_cxt.gin_opCtx = AllocSetContextCreate(CurrentMemoryContext,
        "GIN recovery temporary context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
}

void gin_xlog_cleanup(void)
{
    if (t_thrd.xlog_cxt.gin_opCtx != NULL) {
        MemoryContextDelete(t_thrd.xlog_cxt.gin_opCtx);
        t_thrd.xlog_cxt.gin_opCtx = NULL;
    }
}
