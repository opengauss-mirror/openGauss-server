/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * nbtxlog.cpp
 *    parse btree xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/nbtxlog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "pgxc/pgxc.h"
#include "access/multi_redo_api.h"
#include "miscadmin.h"
#include "access/redo_common.h"

#ifdef ENABLE_UT
#define static
#endif

/*
 * _bt_restore_page -- re-enter all the index tuples on a page
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
void _bt_restore_page(Page page, char *from, int len)
{
    IndexTupleData itupdata;
    Size itemsz;
    char *end = from + len;

    for (; from < end;) {
        /* Need to copy tuple header due to alignment considerations */
        errno_t rc = memcpy_s(&itupdata, sizeof(IndexTupleData), from, sizeof(IndexTupleData));
        securec_check(rc, "\0", "\0");
        itemsz = IndexTupleDSize(itupdata);
        itemsz = MAXALIGN(itemsz);
        if (PageAddItem(page, (Item)from, itemsz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
            ereport(PANIC, (errmsg("_bt_restore_page: cannot add item to page")));
        from += itemsz;
    }
}

void DumpBtreeDeleteInfo(XLogRecPtr lsn, OffsetNumber offsetList[], uint64 offsetNum)
{
    ereport(DEBUG4,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
             errmsg("DumpBtreeDeleteInfo: lsn:%X/%X, offsetnum %lu", (uint32)(lsn >> 32), (uint32)lsn, offsetNum)));
    for (uint64 i = 0; i < offsetNum; ++i) {
        ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                         errmsg("DumpBtreeDeleteInfo: %lu offset %u", i, offsetList[i])));
    }
}

void BtreeRestoreMetaOperatorPage(RedoBufferInfo *metabuf, void *recorddata, Size datalen)
{
    char *ptr = (char *)recorddata;
    Page metapg = metabuf->pageinfo.page;
    BTMetaPageData *md = NULL;
    BTPageOpaqueInternal pageop;
    xl_btree_metadata *xlrec = NULL;

    Assert(datalen == sizeof(xl_btree_metadata));
    Assert(metabuf->blockinfo.blkno == BTREE_METAPAGE);
    xlrec = (xl_btree_metadata *)ptr;

    metapg = metabuf->pageinfo.page;

    _bt_pageinit(metapg, metabuf->pageinfo.pagesize);

    md = BTPageGetMeta(metapg);
    md->btm_magic = BTREE_MAGIC;
    md->btm_version = BTREE_VERSION;
    md->btm_root = xlrec->root;
    md->btm_level = xlrec->level;
    md->btm_fastroot = xlrec->fastroot;
    md->btm_fastlevel = xlrec->fastlevel;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(metapg);
    pageop->btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.	This is not essential
     * but it makes the page look compressible to xlog.c.
     */
    ((PageHeader)metapg)->pd_lower = ((char *)md + sizeof(BTMetaPageData)) - (char *)metapg;

    PageSetLSN(metapg, metabuf->lsn);
}

void BtreeXlogInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *data, Size datalen)
{
    xl_btree_insert *xlrec = (xl_btree_insert *)recorddata;
    Page page = buffer->pageinfo.page;
    char *datapos = (char *)data;

    if (PageAddItem(page, (Item)datapos, datalen, xlrec->offnum, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("btree_insert_redo: failed to add item")));

    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogSplitOperatorRightpage(RedoBufferInfo *rbuf, void *recorddata, BlockNumber leftsib, BlockNumber rnext,
                                     void *blkdata, Size datalen)
{
    xl_btree_split *xlrec = (xl_btree_split *)recorddata;
    bool isleaf = (xlrec->level == 0);
    Page rpage = rbuf->pageinfo.page;
    char *datapos = (char *)blkdata;
    BTPageOpaqueInternal ropaque;

    _bt_pageinit(rpage, rbuf->pageinfo.pagesize);
    ropaque = (BTPageOpaqueInternal)PageGetSpecialPointer(rpage);

    ropaque->btpo_prev = leftsib;
    ropaque->btpo_next = rnext;
    ropaque->btpo.level = xlrec->level;
    ropaque->btpo_flags = isleaf ? BTP_LEAF : 0;
    ropaque->btpo_cycleid = 0;

    _bt_restore_page(rpage, datapos, (int)datalen);

    PageSetLSN(rpage, rbuf->lsn);
}

void BtreeXlogSplitOperatorNextpage(RedoBufferInfo *buffer, BlockNumber rightsib)
{
    Page page = buffer->pageinfo.page;

    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = rightsib;
    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogSplitOperatorLeftpage(RedoBufferInfo *lbuf, void *recorddata, BlockNumber rightsib, bool onleft,
                                    void *blkdata, Size datalen)
{
    xl_btree_split *xlrec = (xl_btree_split *)recorddata;
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
     * but it helps debugging.  See also _bt_restore_page(), which does
     * the same for the right page.
     */

    BTPageOpaqueInternal lopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(lpage);
    OffsetNumber off;
    Item newitem = NULL;
    Size newitemsz = 0;
    Page newlpage;
    OffsetNumber leftoff;

    if (onleft) {
        newitem = (Item)datapos;
        newitemsz = MAXALIGN(IndexTupleSize(newitem));
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
    lopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(lpage);
    lopaque->btpo_flags = BTP_INCOMPLETE_SPLIT;
    if (isleaf) {
        lopaque->btpo_flags |= BTP_LEAF;
    }
    lopaque->btpo_next = rightsib;
    lopaque->btpo_cycleid = 0;

    PageSetLSN(lpage, lbuf->lsn);
}

void BtreeXlogVacuumOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, void *blkdata, Size len)
{
    Page page = redobuffer->pageinfo.page;
    char *ptr = (char *)blkdata;
    BTPageOpaqueInternal opaque;

    if (len > 0) {
        OffsetNumber *unused = NULL;
        OffsetNumber *unend = NULL;

        unused = (OffsetNumber *)ptr;
        unend = (OffsetNumber *)((char *)ptr + len);

        if (module_logging_is_on(MOD_REDO)) {
            DumpBtreeDeleteInfo(redobuffer->lsn, unused, unend - unused);
            DumpPageInfo(page, redobuffer->lsn);
        }

        if ((unend - unused) > 0)
            PageIndexMultiDelete(page, unused, unend - unused);
    }

    /*
     * Mark the page as not containing any LP_DEAD items --- see comments in
     * _bt_delitems_vacuum().
     */
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    PageSetLSN(page, redobuffer->lsn);
    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, redobuffer->lsn);
    }
}

void BtreeXlogDeleteOperatorPage(RedoBufferInfo *buffer, void *recorddata, Size recorddatalen)
{
    xl_btree_delete *xlrec = (xl_btree_delete *)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal opaque;

    if (recorddatalen > SizeOfBtreeDelete) {
        OffsetNumber *unused = NULL;

        unused = (OffsetNumber *)((char *)xlrec + SizeOfBtreeDelete);

        if (module_logging_is_on(MOD_REDO)) {
            DumpPageInfo(page, buffer->lsn);
            DumpBtreeDeleteInfo(buffer->lsn, unused, xlrec->nitems);
        }

        PageIndexMultiDelete(page, unused, xlrec->nitems);
    }

    /*
     * Mark the page as not containing any LP_DEAD items --- see comments in
     * _bt_delitems_delete().
     */
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    PageSetLSN(page, buffer->lsn);

    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, buffer->lsn);
    }
}

void btreeXlogDeletePageOperatorRightpage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_prev = xlrec->leftblk;

    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogDeletePageOperatorLeftpage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_next = xlrec->rightblk;

    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogDeletePageOperatorCurrentpage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop;

    _bt_pageinit(page, buffer->pageinfo.pagesize);
    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = xlrec->leftblk;
    pageop->btpo_next = xlrec->rightblk;
    pageop->btpo_flags = BTP_DELETED;
    pageop->btpo_cycleid = 0;
    ((BTPageOpaque)pageop)->xact = xlrec->btpo_xact;

    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogHalfdeadPageOperatorParentpage(RedoBufferInfo *pbuf, void *recorddata)
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

void BtreeXlogHalfdeadPageOperatorLeafpage(RedoBufferInfo *lbuf, void *recorddata)
{
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)recorddata;

    _bt_pageinit(lbuf->pageinfo.page, lbuf->pageinfo.pagesize);
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(lbuf->pageinfo.page);

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

void BtreeXlogUnlinkPageOperatorRightpage(RedoBufferInfo *rbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(rbuf->pageinfo.page);
    pageop->btpo_prev = xlrec->leftsib;

    PageSetLSN(rbuf->pageinfo.page, rbuf->lsn);
}

void BtreeXlogUnlinkPageOperatorLeftpage(RedoBufferInfo *lbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(lbuf->pageinfo.page);
    pageop->btpo_next = xlrec->rightsib;

    PageSetLSN(lbuf->pageinfo.page, lbuf->lsn);
}

void BtreeXlogUnlinkPageOperatorCurpage(RedoBufferInfo *buf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;

    _bt_pageinit(buf->pageinfo.page, buf->pageinfo.pagesize);
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(buf->pageinfo.page);

    pageop->btpo_prev = xlrec->leftsib;
    pageop->btpo_next = xlrec->rightsib;
    pageop->btpo.xact_old = xlrec->btpo_xact;
    pageop->btpo_flags = BTP_DELETED;
    pageop->btpo_cycleid = 0;

    PageSetLSN(buf->pageinfo.page, buf->lsn);
}

void BtreeXlogUnlinkPageOperatorChildpage(RedoBufferInfo *cbuf, void *recorddata)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)recorddata;

    _bt_pageinit(cbuf->pageinfo.page, cbuf->pageinfo.pagesize);

    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(cbuf->pageinfo.page);

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

void BtreeXlogNewrootOperatorPage(RedoBufferInfo *buffer, void *record, void *blkdata, Size len, BlockNumber *downlink)
{
    xl_btree_newroot *xlrec = (xl_btree_newroot *)record;
    Page page = buffer->pageinfo.page;
    char *ptr = (char *)blkdata;
    BTPageOpaqueInternal pageop;

    _bt_pageinit(page, buffer->pageinfo.pagesize);
    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_flags = BTP_ROOT;
    pageop->btpo_prev = pageop->btpo_next = P_NONE;
    pageop->btpo.level = xlrec->level;
    if (xlrec->level == 0) {
        pageop->btpo_flags |= BTP_LEAF;
    }
    pageop->btpo_cycleid = 0;

    if (xlrec->level > 0) {
        _bt_restore_page(page, ptr, len);
    }

    PageSetLSN(page, buffer->lsn);
}

void BtreeXlogClearIncompleteSplit(RedoBufferInfo *buffer)
{
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    Assert(P_INCOMPLETE_SPLIT(pageop));
    pageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;

    PageSetLSN(page, buffer->lsn);
}

XLogRecParseState *BtreeXlogInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
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

    if (info != XLOG_BTREE_INSERT_LEAF) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_INSERT_CHILD_BLOCK_NUM, blockstate);
    }

    if (info == XLOG_BTREE_INSERT_META) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_INSERT_META_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *BtreeXlogSplitParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_btree_split *xlrec = (xl_btree_split *)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    BlockNumber leftsib;
    BlockNumber rightsib;
    BlockNumber rnext;
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

static XLogRecParseState *BtreeXlogVacuumParseBlock(XLogReaderState *record, uint32 *blocknum)
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
        BlockNumber thisblkno;
        RelFileNode thisrnode;

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

static XLogRecParseState *BtreeXlogDeleteParseBlock(XLogReaderState *record, uint32 *blocknum)
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

static XLogRecParseState *BtreeXlogMarkHalfdeadParseBlock(XLogReaderState *record, uint32 *blocknum)
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

static XLogRecParseState *BtreeXlogUnlinkPageParseBlock(XLogReaderState *record, uint32 *blocknum)
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
    if (info == XLOG_BTREE_UNLINK_PAGE_META) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_UNLINK_PAGE_META_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *BtreeXlogNewrootParseBlock(XLogReaderState *record, uint32 *blocknum)
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

static XLogRecParseState *BtreeXlogReusePageParseBlock(XLogReaderState *record, uint32 *blocknum)
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

XLogRecParseState *BtreeRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
        case XLOG_BTREE_INSERT_UPPER:
        case XLOG_BTREE_INSERT_META:
            recordblockstate = BtreeXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
        case XLOG_BTREE_SPLIT_L_ROOT:
        case XLOG_BTREE_SPLIT_R_ROOT:
            recordblockstate = BtreeXlogSplitParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_VACUUM:
            recordblockstate = BtreeXlogVacuumParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_DELETE:
            recordblockstate = BtreeXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            recordblockstate = BtreeXlogUnlinkPageParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            recordblockstate = BtreeXlogMarkHalfdeadParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_NEWROOT:
            recordblockstate = BtreeXlogNewrootParseBlock(record, blocknum);
            break;
        case XLOG_BTREE_REUSE_PAGE:
            recordblockstate = BtreeXlogReusePageParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("BtreeRedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static void BtreeXlogInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
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

            BtreeXlogInsertOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_INSERT_CHILD_BLOCK_NUM) {
        /* child */
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            BtreeXlogClearIncompleteSplit(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        /* meta */
        BtreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void BtreeXlogSplitBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
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

        BtreeXlogSplitOperatorRightpage(bufferinfo, (void *)maindata, leftsib, rnext, (void *)blkdata, blkdatalen);
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
                bool onleft = ((info == XLOG_BTREE_SPLIT_L) || (info == XLOG_BTREE_SPLIT_L_ROOT));

                blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
                BtreeXlogSplitOperatorLeftpage(bufferinfo, (void *)maindata, rightsib, onleft, (void *)blkdata,
                                               blkdatalen);
            } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM) {
                /* right next */
                BtreeXlogSplitOperatorNextpage(bufferinfo, rightsib);
            } else {
                /* child */
                BtreeXlogClearIncompleteSplit(bufferinfo);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void BtreeXlogVacuumBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        Size blkdatalen = 0;
        char *blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        BtreeXlogVacuumOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void BtreeXlogDeleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size maindatalen;
        char *maindata = XLogBlockDataGetMainData(datadecode, &maindatalen);
        BtreeXlogDeleteOperatorPage(bufferinfo, (void *)maindata, maindatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void BtreeXlogMarkPageHalfdeadBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                           RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == BTREE_HALF_DEAD_LEAF_PAGE_NUM) {
        BtreeXlogHalfdeadPageOperatorLeafpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            BtreeXlogHalfdeadPageOperatorParentpage(bufferinfo, (void *)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void BtreeXlogUnlinkPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                     RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == BTREE_UNLINK_PAGE_CUR_PAGE_NUM) {
        BtreeXlogUnlinkPageOperatorCurpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else if (block_id == BTREE_UNLINK_PAGE_META_NUM) {
        Size blkdatalen;
        char *blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        BtreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    } else if (block_id == BTREE_UNLINK_PAGE_CHILD_NUM) {
        BtreeXlogUnlinkPageOperatorChildpage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            if (block_id == BTREE_UNLINK_PAGE_RIGHT_NUM) {
                BtreeXlogUnlinkPageOperatorRightpage(bufferinfo, (void *)maindata);
            } else {
                BtreeXlogUnlinkPageOperatorLeftpage(bufferinfo, (void *)maindata);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void BtreeXlogNewrootBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                  RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    Size blkdatalen;
    char *blkdata = NULL;
    BlockNumber downlink = 0;

    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_ORIG_BLOCK_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        BtreeXlogNewrootOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen, &downlink);
        MakeRedoBufferDirty(bufferinfo);
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_LEFT_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            BtreeXlogClearIncompleteSplit(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        BtreeRestoreMetaOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void BtreeRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
        case XLOG_BTREE_INSERT_UPPER:
        case XLOG_BTREE_INSERT_META:
            BtreeXlogInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
        case XLOG_BTREE_SPLIT_L_ROOT:
        case XLOG_BTREE_SPLIT_R_ROOT:
            BtreeXlogSplitBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_VACUUM:
            BtreeXlogVacuumBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_DELETE:
            BtreeXlogDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            BtreeXlogUnlinkPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            BtreeXlogMarkPageHalfdeadBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_NEWROOT:
            BtreeXlogNewrootBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("btree_redo_block: unknown op code %u", info)));
    }
}
