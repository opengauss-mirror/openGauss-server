/* -------------------------------------------------------------------------
 *
 * ubtxlog.cpp
 *     parse btree xlog
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/redo/ubtxlog.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "pgxc/pgxc.h"
#include "access/multi_redo_api.h"
#include "miscadmin.h"
#include "access/redo_common.h"
#include "storage/smgr/fd.h"
#include "storage/freespace.h"

#ifdef ENABLE_UT
#define static
#endif

/*
 * UBTreeRestorePage -- re-enter all the index tuples on a page
 *
 * The page is freshly init'd, and *from (length len) is a copy of what
 * had been its upper part (pd_upper to pd_special).  We assume that the
 * tuples had been added to the page in item-number order, and therefore
 * the one with highest item number appears first (lowest on the page).
 *
 * NOTE: the way this routine is coded, the rebuilt page will have the items
 * in correct itemno sequence, but physically the opposite order from the
 * original, because we insert them in the opposite of itemno order.  This
 * does not matter in any current btree code, but it's something to keep an
 * eye on.	Is it worth changing just on general principles?  See also the
 * notes in btree_xlog_split().
 */
void UBTreeRestorePage(Page page, char* from, int len)
{
    IndexTupleData itupdata;
    Size itemsz;
    char* end = from + len;

    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    for (; from < end;) {
        errno_t rc = memcpy_s(&itupdata, sizeof(IndexTupleData), from,
                              sizeof(IndexTupleData));
        securec_check(rc, "\0", "\0");
        itemsz = IndexTupleDSize(itupdata);

        /* every non-pivot tuple in pages which include xmin/xmax will followed by 8B xids */
        if (P_ISLEAF(opaque) && !UBTreeTupleIsPivot((IndexTuple)from)) {
            itemsz += TXNINFOSIZE;
        }

        itemsz = MAXALIGN(itemsz);
        if (PageAddItem(page, (Item)from, itemsz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
            ereport(PANIC, (errmsg("UBTreeRestorePage: cannot add item to page")));
        from += itemsz;
    }
}

void DumpUBTreeDeleteInfo(XLogRecPtr lsn, OffsetNumber offsetList[], uint64 offsetNum)
{
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("DumpUBTreeDeleteInfo: lsn:%X/%X, offsetnum %lu", (uint32)(lsn >> 32), (uint32)lsn, offsetNum)));
    for (uint64 i = 0; i < offsetNum; ++i) {
        ereport(DEBUG4,
                (errmodule(MOD_REDO),
                        errcode(ERRCODE_LOG),
                        errmsg("DumpUBTreeDeleteInfo: %lu offset %u", i, offsetList[i])));
    }
}

void UBTreeRestoreMetaOperatorPage(RedoBufferInfo *metabuf, void *recorddata, Size datalen)
{
    char *ptr = (char *)recorddata;
    Page metapg = metabuf->pageinfo.page;
    BTMetaPageData *md = NULL;
    UBTPageOpaqueInternal pageop;
    xl_btree_metadata *xlrec = NULL;

    Assert(datalen == sizeof(xl_btree_metadata));
    Assert(metabuf->blockinfo.blkno == BTREE_METAPAGE);
    xlrec = (xl_btree_metadata *)ptr;

    metapg = metabuf->pageinfo.page;

    UBTreePageInit(metapg, metabuf->pageinfo.pagesize);

    md = BTPageGetMeta(metapg);
    md->btm_magic = BTREE_MAGIC;
    md->btm_version = BTREE_VERSION;
    md->btm_root = xlrec->root;
    md->btm_level = xlrec->level;
    md->btm_fastroot = xlrec->fastroot;
    md->btm_fastlevel = xlrec->fastlevel;

    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(metapg);
    pageop->btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.	This is not essential
     * but it makes the page look compressible to xlog.c.
     */
    ((PageHeader)metapg)->pd_lower = ((char *)md + sizeof(BTMetaPageData)) - (char *)metapg;

    PageSetLSN(metapg, metabuf->lsn);
}

void UBTreeXlogInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *data, Size datalen)
{
    xl_btree_insert *xlrec = (xl_btree_insert *)recorddata;
    Page page = buffer->pageinfo.page;
    char *datapos = (char *)data;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal) PageGetSpecialPointer(page);
    opaque->activeTupleCount++;

    if (PageAddItem(page, (Item)datapos, datalen, xlrec->offnum, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("btree_insert_redo: failed to add item")));

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogSplitOperatorRightPage(RedoBufferInfo *rbuf, void *recorddata, BlockNumber leftsib, BlockNumber rnext,
                                     void *blkdata, Size datalen, bool hasOpaque)
{
    xl_ubtree_split *xlrec = (xl_ubtree_split *)recorddata;
    bool isleaf = (xlrec->level == 0);
    Page rpage = rbuf->pageinfo.page;
    char *datapos = (char *)blkdata;
    UBTPageOpaqueInternal ropaque;

    UBTreePageInit(rpage, rbuf->pageinfo.pagesize);
    ropaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rpage);

    if (hasOpaque) {
        if (xlrec->opaqueVersion != UBTREE_OPAQUE_VERSION_RCR) {
            ereport(ERROR, (errmsg("unknown btree opaque version")));
        }
        *ropaque = xlrec->ropaque;
    } else {
        ropaque->btpo_prev = leftsib;
        ropaque->btpo_next = rnext;
        ropaque->btpo.level = xlrec->level;
        ropaque->btpo_flags = isleaf ? BTP_LEAF : 0;
        ropaque->btpo_cycleid = 0;
    }

    UBTreeRestorePage(rpage, datapos, (int)datalen);

    PageSetLSN(rpage, rbuf->lsn);
}

void UBTreeXlogSplitOperatorNextpage(RedoBufferInfo *buffer, BlockNumber rightsib)
{
    Page page = buffer->pageinfo.page;

    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = rightsib;
    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogSplitOperatorLeftpage(RedoBufferInfo *lbuf, void *recorddata, BlockNumber rightsib, bool onleft,
                                    void *blkdata, Size datalen, bool hasOpaque)
{
    xl_ubtree_split *xlrec = (xl_ubtree_split *)recorddata;
    bool isleaf = (xlrec->level == 0);
    Page lpage = lbuf->pageinfo.page;
    char *datapos = (char *)blkdata;
    Item left_hikey = NULL;
    Size left_hikeysz = 0;

    /*
     * To retain the same physical order of the tuples that they had, we
     * initialize a temporary empty page for the left page and add all the
     * items to that in item number order.  This mirrors how _bt_split()
     * works.  It's not strictly required to retain the same physical
     * order, as long as the items are in the correct item number order,
     * but it helps debugging.  See also UBTreeRestorePage(), which does
     * the same for the right page.
     */

    UBTPageOpaqueInternal lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);
    OffsetNumber off;
    Item newitem = NULL;
    Size newitemsz = 0;
    Page newlpage;
    OffsetNumber leftoff;

    if (onleft) {
        newitem = (Item)datapos;
        newitemsz = MAXALIGN(IndexTupleSize(newitem));
        if (P_ISLEAF(lopaque)) {
            newitemsz += TXNINFOSIZE;
        }
        datapos += newitemsz;
        datalen -= newitemsz;
    }

    /* Extract left hikey and its size (assuming 16-bit alignment) */
    left_hikey = (Item)datapos;
    left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
    datapos += left_hikeysz;
    datalen -= left_hikeysz;
    Assert(datalen == 0);

    /* assure that memory is properly allocated, prevent from core dump caused by buffer unpin */
    START_CRIT_SECTION();
    newlpage = PageGetTempPageCopySpecial(lpage);
    END_CRIT_SECTION();

    /* Set high key */
    leftoff = P_HIKEY;
    if (PageAddItem(newlpage, left_hikey, left_hikeysz, P_HIKEY, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("failed to add high key to left page after split")));
    leftoff = OffsetNumberNext(leftoff);

    for (off = P_FIRSTDATAKEY(lopaque); off < xlrec->firstright; off++) {
        ItemId itemid;
        Size itemsz;
        Item item;

        /* add the new item if it was inserted on left page */
        if (onleft && off == xlrec->newitemoff) {
            if (PageAddItem(newlpage, newitem, newitemsz, leftoff, false, false) == InvalidOffsetNumber)
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add new item to left page after split")));
            leftoff = OffsetNumberNext(leftoff);
        }

        itemid = PageGetItemId(lpage, off);
        itemsz = ItemIdGetLength(itemid);
        item = PageGetItem(lpage, itemid);
        if (PageAddItem(newlpage, item, itemsz, leftoff, false, false) == InvalidOffsetNumber)
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add old item to left page after split")));
        leftoff = OffsetNumberNext(leftoff);
    }

    /* cope with possibility that newitem goes at the end */
    if (onleft && off == xlrec->newitemoff) {
        if (PageAddItem(newlpage, newitem, newitemsz, leftoff, false, false) == InvalidOffsetNumber)
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add new item to left page after split")));
        leftoff = OffsetNumberNext(leftoff);
    }

    PageRestoreTempPage(newlpage, lpage);

    /* Fix opaque fields */
    if (hasOpaque) {
        if (xlrec->opaqueVersion != UBTREE_OPAQUE_VERSION_RCR) {
            ereport(ERROR, (errmsg("unknown btree opaque version")));
        }
        *lopaque = xlrec->lopaque;
    } else {
        lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);
        lopaque->btpo_flags = BTP_INCOMPLETE_SPLIT;
        if (isleaf) {
            lopaque->btpo_flags |= BTP_LEAF;
        }
        lopaque->btpo_next = rightsib;
        lopaque->btpo_cycleid = 0;
    }

    PageSetLSN(lpage, lbuf->lsn);
}

void UBTreeXlogVacuumOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, void *blkdata, Size len)
{
    Page page = redobuffer->pageinfo.page;
    char *ptr = (char *)blkdata;
    UBTPageOpaqueInternal opaque;

    if (len > 0) {
        OffsetNumber *unused = NULL;
        OffsetNumber *unend = NULL;

        unused = (OffsetNumber *)ptr;
        unend = (OffsetNumber *)((char *)ptr + len);

        if (module_logging_is_on(MOD_REDO)) {
            DumpUBTreeDeleteInfo(redobuffer->lsn, unused, unend - unused);
            DumpPageInfo(page, redobuffer->lsn);
        }

        if ((unend - unused) > 0)
            PageIndexMultiDelete(page, unused, unend - unused);
    }

    /*
     * Mark the page as not containing any LP_DEAD items --- see comments in
     * _bt_delitems_vacuum().
     */
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    PageSetLSN(page, redobuffer->lsn);
    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, redobuffer->lsn);
    }
}

void UBTreeXlogDeleteOperatorPage(RedoBufferInfo *buffer, void *recorddata, Size recorddatalen)
{
    xl_btree_delete *xlrec = (xl_btree_delete *)recorddata;
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal opaque;

    if (recorddatalen > SizeOfBtreeDelete) {
        OffsetNumber *unused = NULL;

        unused = (OffsetNumber *)((char *)xlrec + SizeOfBtreeDelete);

        if (module_logging_is_on(MOD_REDO)) {
            DumpPageInfo(page, buffer->lsn);
            DumpUBTreeDeleteInfo(buffer->lsn, unused, xlrec->nitems);
        }

        PageIndexMultiDelete(page, unused, xlrec->nitems);
    }

    /*
     * Mark the page as not containing any LP_DEAD items --- see comments in
     * _bt_delitems_delete().
     */
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    PageSetLSN(page, buffer->lsn);

    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, buffer->lsn);
    }
}

void UBTreeXlogDeletePageOperatorParentPage(RedoBufferInfo* buffer, void* recorddata, uint8 info)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    OffsetNumber poffset, maxoff;
    UBTPageOpaqueInternal pageop;

    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    poffset = xlrec->poffset;
    maxoff = PageGetMaxOffsetNumber(page);
    if (poffset >= maxoff) {
        Assert(info == XLOG_UBTREE_MARK_PAGE_HALFDEAD);
        Assert(poffset == P_FIRSTDATAKEY(pageop));
        PageIndexTupleDelete(page, poffset);
        pageop->btpo_flags |= BTP_HALF_DEAD;
    } else {
        ItemId itemid;
        IndexTuple itup;
        OffsetNumber nextoffset;

        Assert(info != XLOG_UBTREE_MARK_PAGE_HALFDEAD);
        itemid = PageGetItemId(page, poffset);
        itup = (IndexTuple)PageGetItem(page, itemid);
        UBTreeTupleSetDownLink(itup, xlrec->rightblk);
        nextoffset = OffsetNumberNext(poffset);
        PageIndexTupleDelete(page, nextoffset);
    }

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogDeletePageOperatorRightpage(RedoBufferInfo *buffer, void* recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal pageop;

    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_prev = xlrec->leftblk;

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogDeletePageOperatorLeftpage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal pageop;

    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_next = xlrec->rightblk;

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogDeletePageOperatorCurrentpage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal pageop;

    UBTreePageInit(page, buffer->pageinfo.pagesize);
    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = xlrec->leftblk;
    pageop->btpo_next = xlrec->rightblk;
    pageop->btpo_flags = BTP_DELETED;
    pageop->btpo_cycleid = 0;
    ((UBTPageOpaque)pageop)->xact = xlrec->btpo_xact;

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogHalfdeadPageOperatorParentpage(RedoBufferInfo *pbuf, void *recorddata)
{
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)recorddata;
    OffsetNumber poffset;
    ItemId itemid;
    IndexTuple itup;
    OffsetNumber nextoffset;
    BlockNumber rightsib;

    poffset = xlrec->poffset;

    nextoffset = OffsetNumberNext(poffset);
    itemid = PageGetItemId(pbuf->pageinfo.page, nextoffset);
    itup = (IndexTuple)PageGetItem(pbuf->pageinfo.page, itemid);
    rightsib = ItemPointerGetBlockNumber(&(itup->t_tid));

    itemid = PageGetItemId(pbuf->pageinfo.page, poffset);
    itup = (IndexTuple)PageGetItem(pbuf->pageinfo.page, itemid);
    ItemPointerSetBlockNumber(&(itup->t_tid), rightsib);
    nextoffset = OffsetNumberNext(poffset);
    PageIndexTupleDelete(pbuf->pageinfo.page, nextoffset);

    PageSetLSN(pbuf->pageinfo.page, pbuf->lsn);
}

void UBTreeXlogHalfdeadPageOperatorLeafpage(RedoBufferInfo *lbuf, void *recorddata)
{
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)recorddata;

    UBTreePageInit(lbuf->pageinfo.page, lbuf->pageinfo.pagesize);
    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(lbuf->pageinfo.page);

    pageop->btpo_prev = xlrec->leftblk;
    pageop->btpo_next = xlrec->rightblk;

    pageop->btpo.level = 0;
    pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
    pageop->btpo_cycleid = 0;

    /*
     * Construct a dummy hikey item that points to the next parent to be
     * deleted (if any).
     */
    IndexTupleData trunctuple;
    errno_t rc = memset_s(&trunctuple, sizeof(IndexTupleData), 0, sizeof(IndexTupleData));
    securec_check(rc, "\0", "\0");
    trunctuple.t_info = sizeof(IndexTupleData);
    ItemPointerSet(&(trunctuple.t_tid), xlrec->topparent, 0);

    if (PageAddItem(lbuf->pageinfo.page, (Item)&trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) ==
        InvalidOffsetNumber) {
        ereport(ERROR, (errmsg("could not add dummy high key to half-dead page")));
    }

    PageSetLSN(lbuf->pageinfo.page, lbuf->lsn);
}

void UBTreeXlogUnlinkPageOperatorRightpage(RedoBufferInfo *rbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;
    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(rbuf->pageinfo.page);
    pageop->btpo_prev = xlrec->leftsib;

    PageSetLSN(rbuf->pageinfo.page, rbuf->lsn);
}

void UBTreeXlogUnlinkPageOperatorLeftpage(RedoBufferInfo *lbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;
    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(lbuf->pageinfo.page);
    pageop->btpo_next = xlrec->rightsib;

    PageSetLSN(lbuf->pageinfo.page, lbuf->lsn);
}

void UBTreeXlogUnlinkPageOperatorCurpage(RedoBufferInfo *buf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;

    UBTreePageInit(buf->pageinfo.page, buf->pageinfo.pagesize);
    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(buf->pageinfo.page);

    pageop->btpo_prev = xlrec->leftsib;
    pageop->btpo_next = xlrec->rightsib;
    pageop->btpo.xact_old = xlrec->btpo_xact;
    pageop->btpo_flags = BTP_DELETED;
    pageop->btpo_cycleid = 0;

    PageSetLSN(buf->pageinfo.page, buf->lsn);
}

void UBTreeXlogUnlinkPageOperatorChildpage(RedoBufferInfo *cbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;

    UBTreePageInit(cbuf->pageinfo.page, cbuf->pageinfo.pagesize);

    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(cbuf->pageinfo.page);

    pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
    pageop->btpo_prev = xlrec->leafleftsib;
    pageop->btpo_next = xlrec->leafrightsib;
    pageop->btpo.level = 0;
    pageop->btpo_cycleid = 0;

    /* Add a dummy hikey item */
    IndexTupleData trunctuple;
    errno_t rc = memset_s(&trunctuple, sizeof(IndexTupleData), 0, sizeof(IndexTupleData));
    securec_check(rc, "\0", "\0");
    trunctuple.t_info = sizeof(IndexTupleData);
    ItemPointerSet(&(trunctuple.t_tid), xlrec->topparent, 0);

    if (PageAddItem(cbuf->pageinfo.page, (Item)&trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) ==
        InvalidOffsetNumber) {
        ereport(ERROR, (errmsg("could not add dummy high key to half-dead page")));
    }

    PageSetLSN(cbuf->pageinfo.page, cbuf->lsn);
}

void UBTreeXlogNewrootOperatorPage(RedoBufferInfo *buffer, void *record, void *blkdata, Size len, BlockNumber *downlink)
{
    xl_btree_newroot *xlrec = (xl_btree_newroot *)record;
    Page page = buffer->pageinfo.page;
    char *ptr = (char *)blkdata;
    UBTPageOpaqueInternal pageop;

    UBTreePageInit(page, buffer->pageinfo.pagesize);
    pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_flags = BTP_ROOT;
    pageop->btpo_prev = pageop->btpo_next = P_NONE;
    pageop->btpo.level = xlrec->level;
    if (xlrec->level == 0) {
        pageop->btpo_flags |= BTP_LEAF;
    }
    pageop->btpo_cycleid = 0;

    if (xlrec->level > 0) {
        UBTreeRestorePage(page, ptr, len);
    }

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogClearIncompleteSplit(RedoBufferInfo *buffer)
{
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal pageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    Assert(P_INCOMPLETE_SPLIT(pageop));
    pageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;

    PageSetLSN(page, buffer->lsn);
}

XLogRecParseState *UBTreeXlogInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_INSERT_ORIG_BLOCK_NUM, recordstatehead);

    if (info != XLOG_UBTREE_INSERT_LEAF) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_INSERT_CHILD_BLOCK_NUM, blockstate);
    }

    if (info == XLOG_UBTREE_INSERT_META) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_INSERT_META_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogSplitParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_ubtree_split *xlrec = (xl_ubtree_split *)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    BlockNumber leftsib = InvalidBlockNumber;
    BlockNumber rightsib = InvalidBlockNumber;
    BlockNumber rnext = InvalidBlockNumber;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    XLogRecGetBlockTag(record, BTREE_SPLIT_LEFT_BLOCK_NUM, NULL, NULL, &leftsib);
    XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, NULL, NULL, &rightsib);
    if (!XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, NULL, NULL, &rnext))
        rnext = P_NONE;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_SPLIT_LEFT_BLOCK_NUM, recordstatehead);
    XLogRecSetAuxiBlkNumState(&recordstatehead->blockparse.extra_rec.blockdatarec, rightsib, InvalidForkNumber);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, blockstate);
    XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, rnext, leftsib);

    if (rnext != P_NONE) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, blockstate);
        XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, rightsib, InvalidForkNumber);
    }

    if (!isleaf) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_SPLIT_CHILD_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogVacuumParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_VACUUM_ORIG_BLOCK_NUM, recordstatehead);

    if (g_supportHotStandby) {
        BlockNumber thisblkno = InvalidBlockNumber;
        RelFileNode thisrnode = ((RelFileNode) {0, 0, 0, -1});

        xl_btree_vacuum *xlrec = (xl_btree_vacuum *)XLogRecGetData(record);
        XLogRecGetBlockTag(record, BTREE_VACUUM_ORIG_BLOCK_NUM, &thisrnode, NULL, &thisblkno);

        if ((xlrec->lastBlockVacuumed + 1) < thisblkno) {
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }

            RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&thisrnode, InvalidBackendId, MAIN_FORKNUM, thisblkno);
            XLogRecSetBlockCommonState(record, BLOCK_DATA_VACUUM_PIN_TYPE, filenode, blockstate);
            XLogRecSetPinVacuumState(&blockstate->blockparse.extra_rec.blockvacuumpin, xlrec->lastBlockVacuumed);
        }
    }

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogDeleteParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_DELETE_ORIG_BLOCK_NUM, recordstatehead);

    /* for hot standby, need to reslove the conflict */
    {
        /* wait for syn with pg > 9.6 */
    }
    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogMarkHalfdeadParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_HALF_DEAD_PARENT_PAGE_NUM, recordstatehead);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_HALF_DEAD_LEAF_PAGE_NUM, blockstate);

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogUnlinkPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)XLogRecGetData(record);
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_RIGHT_NUM, recordstatehead);

    if (xlrec->leftsib != P_NONE) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_LEFT_NUM, blockstate);
    }

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_CUR_PAGE_NUM, blockstate);

    if (XLogRecHasBlockRef(record, BTREE_UNLINK_PAGE_CHILD_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_CHILD_NUM, blockstate);
    }

    /* Update metapage if needed */
    if (info == XLOG_UBTREE_UNLINK_PAGE_META) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_META_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogNewrootParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_btree_newroot *xlrec = (xl_btree_newroot *)XLogRecGetData(record);
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_NEWROOT_ORIG_BLOCK_NUM, recordstatehead);

    if (xlrec->level > 0) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_NEWROOT_LEFT_BLOCK_NUM, blockstate);
    }

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_NEWROOT_META_BLOCK_NUM, blockstate);

    return recordstatehead;
}

static XLogRecParseState *UBTreeXlogReusePageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *)XLogRecGetData(record);

    *blocknum = 0;
    if (g_supportHotStandby) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
        if (recordstatehead == NULL) {
            return NULL;
        }

        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->node, XLogRecGetBucketId(record));

        RelFileNodeForkNum filenode =
                RelFileNodeForkNumFill(&rnode, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
        XLogRecSetBlockCommonState(record, BLOCK_DATA_INVALIDMSG_TYPE, filenode, recordstatehead);
        XLogRecSetInvalidMsgState(&recordstatehead->blockparse.extra_rec.blockinvalidmsg, xlrec->latestRemovedXid);
    }
    return recordstatehead;
}

static XLogRecParseState* UBTreeXlogMarkDeleteParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, UBTREE_MARK_DELETE_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState* UBTreeXlogPrunePageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, UBTREE_PAGE_PRUNE_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState* UBTree2XlogShiftBaseParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    *blocknum = 1;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, UBTREE2_BASE_SHIFT_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UBTree2XlogRecycleQueueInitPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_ubtree2_recycle_queue_init_page *xlrec = (xl_ubtree2_recycle_queue_init_page *)XLogRecGetData(record);
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_INIT_PAGE_CURR_BLOCK_NUM, recordstatehead);

    if (xlrec->insertingNewPage) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_INIT_PAGE_LEFT_BLOCK_NUM, blockstate);

        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_INIT_PAGE_RIGHT_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

XLogRecParseState *UBTree2XlogRecycleQueueEndpointParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_ENDPOINT_CURR_BLOCK_NUM, recordstatehead);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_ENDPOINT_NEXT_BLOCK_NUM, blockstate);

    return recordstatehead;
}

XLogRecParseState *UBTree2XlogRecycleQueueModifyParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UBTREE2_RECYCLE_QUEUE_MODIFY_BLOCK_NUM, recordstatehead);

    return recordstatehead;
}

XLogRecParseState *UBTree2XlogFreezeParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UBTREE2_FREEZE_BLOCK_NUM, recordstatehead);

    return recordstatehead;
}

XLogRecParseState *UBTreeRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_UBTREE_INSERT_LEAF:
        case XLOG_UBTREE_INSERT_UPPER:
        case XLOG_UBTREE_INSERT_META:
            recordblockstate = UBTreeXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_SPLIT_L:
        case XLOG_UBTREE_SPLIT_R:
        case XLOG_UBTREE_SPLIT_L_ROOT:
        case XLOG_UBTREE_SPLIT_R_ROOT:
            recordblockstate = UBTreeXlogSplitParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_VACUUM:
            recordblockstate = UBTreeXlogVacuumParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_DELETE:
            recordblockstate = UBTreeXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_UNLINK_PAGE:
        case XLOG_UBTREE_UNLINK_PAGE_META:
            recordblockstate = UBTreeXlogUnlinkPageParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_MARK_PAGE_HALFDEAD:
            recordblockstate = UBTreeXlogMarkHalfdeadParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_NEWROOT:
            recordblockstate = UBTreeXlogNewrootParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_REUSE_PAGE:
            recordblockstate = UBTreeXlogReusePageParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_MARK_DELETE:
            recordblockstate = UBTreeXlogMarkDeleteParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE_PRUNE_PAGE:
            recordblockstate = UBTreeXlogPrunePageParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UBTreeRedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

XLogRecParseState* UBTree2RedoParseToBlock(XLogReaderState* record, uint32* blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_UBTREE2_SHIFT_BASE:
            recordblockstate = UBTree2XlogShiftBaseParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE:
            recordblockstate = UBTree2XlogRecycleQueueInitPageParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT:
            recordblockstate = UBTree2XlogRecycleQueueEndpointParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY:
            recordblockstate = UBTree2XlogRecycleQueueModifyParseBlock(record, blocknum);
            break;
        case XLOG_UBTREE2_FREEZE:
            recordblockstate = UBTree2XlogFreezeParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UBTree2RedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static void UBTreeXlogInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    Size blkdatalen;
    char *blkdata = NULL;
    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_INSERT_ORIG_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            Assert(blkdata != NULL);
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

            UBTreeXlogInsertOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_INSERT_CHILD_BLOCK_NUM) {
        /* child */
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            UBTreeXlogClearIncompleteSplit(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        /* meta */
        UBTreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTreeXlogSplitBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_RIGHT_BLOCK_NUM) {
        /* right page */
        BlockNumber leftsib;
        BlockNumber rnext;
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        rnext = XLogBlockDataGetAuxiBlock1(datadecode);
        leftsib = XLogBlockDataGetAuxiBlock2(datadecode);

        UBTreeXlogSplitOperatorRightPage(bufferinfo, (void *)maindata, leftsib, rnext, (void *)blkdata,
            blkdatalen, true);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            BlockNumber rightsib;
            rightsib = XLogBlockDataGetAuxiBlock1(datadecode);

            if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_LEFT_BLOCK_NUM) {
                /* left page */
                Size blkdatalen;
                char *blkdata = NULL;
                char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
                bool onleft = ((info == XLOG_UBTREE_SPLIT_L) || (info == XLOG_UBTREE_SPLIT_L_ROOT));

                blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
                UBTreeXlogSplitOperatorLeftpage(bufferinfo, (void *)maindata, rightsib, onleft, (void *)blkdata,
                    blkdatalen, true);
            } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM) {
                /* right next */
                UBTreeXlogSplitOperatorNextpage(bufferinfo, rightsib);
            } else {
                /* child */
                UBTreeXlogClearIncompleteSplit(bufferinfo);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTreeXlogVacuumBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        Size blkdatalen = 0;
        char *blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        UBTreeXlogVacuumOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTreeXlogDeleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size maindatalen;
        char *maindata = XLogBlockDataGetMainData(datadecode, &maindatalen);
        UBTreeXlogDeleteOperatorPage(bufferinfo, (void *)maindata, maindatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTreeXlogMarkPageHalfdeadBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                           RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == BTREE_HALF_DEAD_LEAF_PAGE_NUM) {
        UBTreeXlogHalfdeadPageOperatorLeafpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            UBTreeXlogHalfdeadPageOperatorParentpage(bufferinfo, (void *)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTreeXlogUnlinkPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                     RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == BTREE_UNLINK_PAGE_CUR_PAGE_NUM) {
        UBTreeXlogUnlinkPageOperatorCurpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else if (block_id == BTREE_UNLINK_PAGE_META_NUM) {
        Size blkdatalen;
        char *blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        UBTreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    } else if (block_id == BTREE_UNLINK_PAGE_CHILD_NUM) {
        UBTreeXlogUnlinkPageOperatorChildpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            if (block_id == BTREE_UNLINK_PAGE_RIGHT_NUM) {
                UBTreeXlogUnlinkPageOperatorRightpage(bufferinfo, (void *)maindata);
            } else {
                UBTreeXlogUnlinkPageOperatorLeftpage(bufferinfo, (void *)maindata);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTreeXlogNewrootBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                  RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    Size blkdatalen;
    char *blkdata = NULL;
    BlockNumber downlink = 0;

    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_ORIG_BLOCK_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        UBTreeXlogNewrootOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen, &downlink);
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_LEFT_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            UBTreeXlogClearIncompleteSplit(bufferinfo);
        }
    } else {
        UBTreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
    }
    MakeRedoBufferDirty(bufferinfo);
}

void UBTreeXlogMarkDeleteOperatorPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree_mark_delete *xlrec = (xl_ubtree_mark_delete *)recorddata;
    Page page = buffer->pageinfo.page;

    UBTPageOpaqueInternal opaque;
    IndexTuple itup;
    UstoreIndexXid uxid;
    TransactionId xid = xlrec->xid;
    OffsetNumber offset = xlrec->offset;

    page = buffer->pageinfo.page;
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offset));
    uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);

    uxid->xmax = NormalTransactionIdToShort(opaque->xid_base, xid);

    /* update active hint */
    opaque->activeTupleCount--;

    if (TransactionIdPrecedes(opaque->last_delete_xid, xid))
        opaque->last_delete_xid = xid;

    PageSetLSN(page, buffer->lsn);
}

void UBTreeXlogPrunePageOperatorPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree_prune_page *xlrec = (xl_ubtree_prune_page *)recorddata;
    Page page = buffer->pageinfo.page;

    /* Set up flags and try to repair page  fragmentation */
    UBTreePagePruneExecute(page, (OffsetNumber *)(((char *)xlrec) + SizeOfUBTreePrunePage), xlrec->count, NULL);

    UBTreePageRepairFragmentation(page);

    /*
     * Update the page's pd_prune_xid field to either zero, or the lowest
     * XID of any soon-prunable tuple.
     */

    PageSetPruneXid(page, xlrec->new_prune_xid);
    PageSetLSN(page, buffer->lsn);
}

void UBTree2XlogShiftBaseOperatorPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree2_shift_base *xlrec = (xl_ubtree2_shift_base *)recorddata;
    Page page = buffer->pageinfo.page;

    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    int64 delta = xlrec->delta;

    /* base left shift, mininum is 0 */
    if (delta < 0) {
        if ((int64) (opaque->xid_base + delta) < 0) {
            delta = -(int64)(opaque->xid_base);
        }
    }

    /* reset pd_prune_xid for prune hint */
    ((PageHeader)page)->pd_prune_xid = InvalidTransactionId;

    /* Iterate over page items */
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        IndexTuple itup;
        UstoreIndexXid uxid;

        itemid = PageGetItemId(page, offnum);
        if (!ItemIdHasStorage(itemid)) {
            continue;
        }

        itup = (IndexTuple) PageGetItem(page, itemid);
        uxid = (UstoreIndexXid) UstoreIndexTupleGetXid(itup);
        /* Apply xid shift to heap tuple */
        if (TransactionIdIsNormal(uxid->xmin)) {
            Assert((uint32)(uxid->xmin - delta) >= FirstNormalTransactionId);
            Assert((uint32)(uxid->xmin - delta) <= MaxShortTransactionId);
            uxid->xmin -= delta;
        }

        if (TransactionIdIsNormal(uxid->xmax)) {
            Assert((uint32)(uxid->xmax - delta) >= FirstNormalTransactionId);
            Assert((uint32)(uxid->xmax - delta) <= MaxShortTransactionId);
            uxid->xmax -= delta;
        }
    }

    /* Apply xid shift to base as well */
    opaque->xid_base += delta;

    PageSetLSN(page, buffer->lsn);
}

void UBTree2XlogRecycleQueueInitPageOperatorCurrPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree2_recycle_queue_init_page *xlrec = (xl_ubtree2_recycle_queue_init_page *)recorddata;
    Page page = BufferGetPage(buffer->buf);
    UBTreeRecycleQueueInitPage(NULL, page, xlrec->currBlkno, xlrec->prevBlkno, xlrec->nextBlkno);
    PageSetLSN(page, buffer->lsn);
}

void UBTree2XlogRecycleQueueInitPageOperatorAdjacentPage(RedoBufferInfo* buffer, void* recorddata, bool isLeft)
{
    xl_ubtree2_recycle_queue_init_page *xlrec = (xl_ubtree2_recycle_queue_init_page *)recorddata;
    UBtreeRecycleQueueChangeChain(buffer->buf, xlrec->currBlkno, isLeft);
    PageSetLSN(BufferGetPage(buffer->buf), buffer->lsn);
}

void UBTree2XlogRecycleQueueEndpointOperatorLeftPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree2_recycle_queue_endpoint *xlrec = (xl_ubtree2_recycle_queue_endpoint *)recorddata;
    UBTreeRecycleQueuePageChangeEndpointLeftPage(buffer->buf, xlrec->isHead);
    PageSetLSN(BufferGetPage(buffer->buf), buffer->lsn);
}

void UBTree2XlogRecycleQueueEndpointOperatorRightPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree2_recycle_queue_endpoint *xlrec = (xl_ubtree2_recycle_queue_endpoint *)recorddata;
    UBTreeRecycleQueuePageChangeEndpointRightPage(buffer->buf, xlrec->isHead);
    PageSetLSN(BufferGetPage(buffer->buf), buffer->lsn);
}

void UBTree2XlogRecycleQueueModifyOperatorPage(RedoBufferInfo* buffer, void* recorddata)
{
    UBTreeXlogRecycleQueueModifyPage(buffer->buf, (xl_ubtree2_recycle_queue_modify *)recorddata);
    PageSetLSN(BufferGetPage(buffer->buf), buffer->lsn);
}

void UBTree2XlogFreezeOperatorPage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_ubtree2_freeze *xlrec = (xl_ubtree2_freeze *)recorddata;
    Page page = buffer->pageinfo.page;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* freeze all tuples recorded in nowFrozen */
    OffsetNumber *nowFrozen = (OffsetNumber *)(((char *)xlrec) + SizeOfUBTree2Freeze);
    for (int i = 0; i < xlrec->nfrozen; i++) {
        OffsetNumber offnum = nowFrozen[i];
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid) UstoreIndexTupleGetXid(itup);
        /* freeze xmin */
        uxid->xmin = FrozenTransactionId;
    }
    /* Iterate over page items, remove xmax if necessary */
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (!ItemIdHasStorage(itemid)) {
            continue;
        }
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
        if (TransactionIdIsNormal(uxid->xmax)) {
            TransactionId xmax = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmax);
            if (TransactionIdPrecedes(xmax, xlrec->oldestXmin)) {
                uxid->xmax = InvalidTransactionId; /* aborted */
            }
        }
    }
    PageSetLSN(page, buffer->lsn);
}

static void UBTreeXlogMarkDeleteBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == UBTREE_MARK_DELETE_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            char* maindata = XLogBlockDataGetMainData(datadecode, NULL);
            UBTreeXlogMarkDeleteOperatorPage(bufferinfo, (void*)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTreeXlogPrunePageBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == UBTREE_PAGE_PRUNE_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
            UBTreeXlogPrunePageOperatorPage(bufferinfo, (void*)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTree2XlogBaseShiftBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_BASE_SHIFT_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
            UBTree2XlogShiftBaseOperatorPage(bufferinfo, (void*)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void UBTree2XlogRecycleQueueInitPageBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_INIT_PAGE_CURR_BLOCK_NUM) {
            UBTree2XlogRecycleQueueInitPageOperatorCurrPage(bufferinfo, (void*)maindata);
        } else if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_INIT_PAGE_LEFT_BLOCK_NUM) {
            UBTree2XlogRecycleQueueInitPageOperatorAdjacentPage(bufferinfo, (void*)maindata, true);
        } else if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_INIT_PAGE_RIGHT_BLOCK_NUM) {
            UBTree2XlogRecycleQueueInitPageOperatorAdjacentPage(bufferinfo, (void*)maindata, false);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTree2XlogRecycleQueueEndpointBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_ENDPOINT_CURR_BLOCK_NUM) {
            UBTree2XlogRecycleQueueEndpointOperatorLeftPage(bufferinfo, (void*)maindata);
        } else if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_ENDPOINT_NEXT_BLOCK_NUM) {
            UBTree2XlogRecycleQueueEndpointOperatorRightPage(bufferinfo, (void*)maindata);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTree2XlogRecycleQueueModifyBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_RECYCLE_QUEUE_MODIFY_BLOCK_NUM) {
            UBTree2XlogRecycleQueueModifyOperatorPage(bufferinfo, (void*)maindata);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UBTree2XlogFreezeBlock(
        XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        if (XLogBlockDataGetBlockId(datadecode) == UBTREE2_FREEZE_BLOCK_NUM) {
            UBTree2XlogFreezeOperatorPage(bufferinfo, (void*)maindata);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

void UBTree2RedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UBTREE2_SHIFT_BASE:
            UBTree2XlogBaseShiftBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE:
            UBTree2XlogRecycleQueueInitPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT:
            UBTree2XlogRecycleQueueEndpointBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY:
            UBTree2XlogRecycleQueueModifyBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE2_FREEZE:
            UBTree2XlogFreezeBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("UBTree2RedoDataBlock: unknown op code %u", info)));
    }
}

void UBTreeRedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UBTREE_INSERT_LEAF:
        case XLOG_UBTREE_INSERT_UPPER:
        case XLOG_UBTREE_INSERT_META:
            UBTreeXlogInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_SPLIT_L:
        case XLOG_UBTREE_SPLIT_R:
        case XLOG_UBTREE_SPLIT_L_ROOT:
        case XLOG_UBTREE_SPLIT_R_ROOT:
            UBTreeXlogSplitBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_VACUUM:
            UBTreeXlogVacuumBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_DELETE:
            UBTreeXlogDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_UNLINK_PAGE:
        case XLOG_UBTREE_UNLINK_PAGE_META:
            UBTreeXlogUnlinkPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_MARK_PAGE_HALFDEAD:
            UBTreeXlogMarkPageHalfdeadBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_NEWROOT:
            UBTreeXlogNewrootBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_MARK_DELETE:
            UBTreeXlogMarkDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UBTREE_PRUNE_PAGE:
            UBTreeXlogPrunePageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("UBTreeRedoDataBlock: unknown op code %u", info)));
    }
}
