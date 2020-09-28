/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 *
 * src/gausskernel/storage/access/redo/nbtxlog.cpp
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

enum {
    BTREE_INSERT_ORIG_BLOCK_NUM = 0,
    BTREE_INSERT_CHILD_BLOCK_NUM,
    BTREE_INSERT_META_BLOCK_NUM
};

enum {
    BTREE_SPLIT_LEFT_BLOCK_NUM = 0,
    BTREE_SPLIT_RIGHT_BLOCK_NUM,
    BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM,
    BTREE_SPLIT_CHILD_BLOCK_NUM
};

enum {
    BTREE_VACUUM_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_DELETE_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_DELETE_PAGE_TARGET_BLOCK_NUM = 0,
    BTREE_DELETE_PAGE_LEFT_BLOCK_NUM,
    BTREE_DELETE_PAGE_RIGHT_BLOCK_NUM,
    BTREE_DELETE_PAGE_PARENT_BLOCK_NUM,
    BTREE_DELETE_PAGE_META_BLOCK_NUM,
};

enum {
    BTREE_NEWROOT_ORIG_BLOCK_NUM = 0,
    BTREE_NEWROOT_LEFT_BLOCK_NUM,
    BTREE_NEWROOT_META_BLOCK_NUM,
};

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
void _bt_restore_page(Page page, char* from, int len)
{
    IndexTupleData itupdata;
    Size itemsz;
    char* end = from + len;

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
        (errmodule(MOD_REDO),
            errcode(ERRCODE_LOG),
            errmsg("DumpBtreeDeleteInfo: lsn:%X/%X, offsetnum %lu", (uint32)(lsn >> 32), (uint32)lsn, offsetNum)));
    for (uint64 i = 0; i < offsetNum; ++i) {
        ereport(DEBUG4,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("DumpBtreeDeleteInfo: %lu offset %u", i, offsetList[i])));
    }
}

void btree_restore_meta_operator_page(RedoBufferInfo* metabuf, void* recorddata, Size datalen)
{
    char* ptr = (char*)recorddata;
    Page metapg = metabuf->pageinfo.page;
    BTMetaPageData* md = NULL;
    BTPageOpaqueInternal pageop;
    xl_btree_metadata* xlrec = NULL;

    Assert(datalen == sizeof(xl_btree_metadata));
    Assert(metabuf->blockinfo.blkno == BTREE_METAPAGE);
    xlrec = (xl_btree_metadata*)ptr;

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
    ((PageHeader)metapg)->pd_lower = ((char*)md + sizeof(BTMetaPageData)) - (char*)metapg;

    PageSetLSN(metapg, metabuf->lsn);
}

void btree_xlog_insert_operator_page(RedoBufferInfo* buffer, void* recorddata, void* data, Size datalen)
{
    xl_btree_insert* xlrec = (xl_btree_insert*)recorddata;
    Page page = buffer->pageinfo.page;
    char* datapos = (char*)data;

    if (PageAddItem(page, (Item)datapos, datalen, xlrec->offnum, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("btree_insert_redo: failed to add item")));

    PageSetLSN(page, buffer->lsn);
}

void btree_xlog_split_operator_rightpage(
    RedoBufferInfo* rbuf, void* recorddata, BlockNumber leftsib, BlockNumber rnext, void* blkdata, Size datalen)
{
    xl_btree_split* xlrec = (xl_btree_split*)recorddata;
    bool isleaf = (xlrec->level == 0);
    Page rpage = rbuf->pageinfo.page;
    char* datapos = (char*)blkdata;
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

void btree_xlog_split_operator_nextpage(RedoBufferInfo* buffer, BlockNumber rightsib)
{
    Page page = buffer->pageinfo.page;

    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = rightsib;
    PageSetLSN(page, buffer->lsn);
}

void btree_xlog_split_operator_leftpage(RedoBufferInfo* lbuf, void* recorddata, BlockNumber rightsib, bool onleft,
    void* blkdata, Size datalen, Item input_left_hikey, Size input_left_hikeysz)
{
    xl_btree_split* xlrec = (xl_btree_split*)recorddata;
    bool isleaf = (xlrec->level == 0);
    Page lpage = lbuf->pageinfo.page;
    char* datapos = (char*)blkdata;
    Item left_hikey = input_left_hikey;
    Size left_hikeysz = input_left_hikeysz;

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
    newlpage = PageGetTempPageCopySpecial(lpage, true);
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
            ereport(
                ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add old item to left page after split")));
        leftoff = OffsetNumberNext(leftoff);
    }

    /* cope with possibility that newitem goes at the end */
    if (onleft && off == xlrec->newitemoff) {
        if (PageAddItem(newlpage, newitem, newitemsz, leftoff, false, false) == InvalidOffsetNumber)
            ereport(
                ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add new item to left page after split")));
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

void btree_xlog_vacuum_operator_page(RedoBufferInfo* redobuffer, void* recorddata, void* blkdata, Size len)
{
    Page page = redobuffer->pageinfo.page;
    char* ptr = (char*)blkdata;
    BTPageOpaqueInternal opaque;

    if (len > 0) {
        OffsetNumber* unused = NULL;
        OffsetNumber* unend = NULL;

        unused = (OffsetNumber*)ptr;
        unend = (OffsetNumber*)((char*)ptr + len);

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

void btree_xlog_delete_operator_page(RedoBufferInfo* buffer, void* recorddata, Size recorddatalen)
{
    xl_btree_delete* xlrec = (xl_btree_delete*)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal opaque;

    if (recorddatalen > SizeOfBtreeDelete) {
        OffsetNumber* unused = NULL;

        unused = (OffsetNumber*)((char*)xlrec + SizeOfBtreeDelete);

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

void btree_xlog_delete_page_operator_parentpage(RedoBufferInfo* buffer, void* recorddata, uint8 info)
{
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)recorddata;
    Page page = buffer->pageinfo.page;
    OffsetNumber poffset, maxoff;
    BTPageOpaqueInternal pageop;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    poffset = xlrec->poffset;
    maxoff = PageGetMaxOffsetNumber(page);
    if (poffset >= maxoff) {
        Assert(info == XLOG_BTREE_DELETE_PAGE_HALF);
        Assert(poffset == P_FIRSTDATAKEY(pageop));
        PageIndexTupleDelete(page, poffset);
        pageop->btpo_flags |= BTP_HALF_DEAD;
    } else {
        ItemId itemid;
        IndexTuple itup;
        OffsetNumber nextoffset;

        Assert(info != XLOG_BTREE_DELETE_PAGE_HALF);
        itemid = PageGetItemId(page, poffset);
        itup = (IndexTuple)PageGetItem(page, itemid);
        BTreeInnerTupleSetDownLink(itup, xlrec->rightblk);
        nextoffset = OffsetNumberNext(poffset);
        PageIndexTupleDelete(page, nextoffset);
    }

    PageSetLSN(page, buffer->lsn);
}

void btree_xlog_delete_page_operator_rightpage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_prev = xlrec->leftblk;

    PageSetLSN(page, buffer->lsn);
}

void btree_xlog_delete_page_operator_leftpage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)recorddata;
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop;

    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_next = xlrec->rightblk;

    PageSetLSN(page, buffer->lsn);
}

void btree_xlog_delete_page_operator_currentpage(RedoBufferInfo* buffer, void* recorddata)
{
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)recorddata;
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

void btree_xlog_newroot_operator_page(
    RedoBufferInfo* buffer, void* record, void* blkdata, Size len, BlockNumber* downlink)
{
    xl_btree_newroot* xlrec = (xl_btree_newroot*)record;
    Page page = buffer->pageinfo.page;
    char* ptr = (char*)blkdata;
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

void btree_xlog_clear_incomplete_split(RedoBufferInfo* buffer)
{
    Page page = buffer->pageinfo.page;
    BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    Assert(P_INCOMPLETE_SPLIT(pageop));
    pageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;

    PageSetLSN(page, buffer->lsn);
}

XLogRecParseState* btree_xlog_insert_parse_block(XLogReaderState* record, uint32* blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState* recordstatehead = NULL;
    XLogRecParseState* blockstate = NULL;

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

static XLogRecParseState* btree_xlog_split_parse_block(XLogReaderState* record, uint32* blocknum)
{
    xl_btree_split* xlrec = (xl_btree_split*)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    BlockNumber leftsib;
    BlockNumber rightsib;
    BlockNumber rnext;
    XLogRecParseState* recordstatehead = NULL;
    XLogRecParseState* blockstate = NULL;

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

static XLogRecParseState* btree_xlog_vacuum_parse_block(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogRecParseState* blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_VACUUM_ORIG_BLOCK_NUM, recordstatehead);

    if (SUPPORT_HOT_STANDBY) {
        BlockNumber thisblkno;
        RelFileNode thisrnode;

        xl_btree_vacuum* xlrec = (xl_btree_vacuum*)XLogRecGetData(record);
        XLogRecGetBlockTag(record, BTREE_VACUUM_ORIG_BLOCK_NUM, &thisrnode, NULL, &thisblkno);

        if ((xlrec->lastBlockVacuumed + 1) < thisblkno) {
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }

            XLogRecSetBlockCommonState(
                record, BLOCK_DATA_VACUUM_PIN_TYPE, MAIN_FORKNUM, thisblkno, &thisrnode, blockstate);
            XLogRecSetPinVacuumState(&blockstate->blockparse.extra_rec.blockvacuumpin, xlrec->lastBlockVacuumed);
        }
    }

    return recordstatehead;
}

static XLogRecParseState* btree_xlog_delete_parse_block(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;

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

static XLogRecParseState* btree_xlog_delete_page_parse_block(XLogReaderState* record, uint32* blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)XLogRecGetData(record);
    XLogRecParseState* recordstatehead = NULL;
    XLogRecParseState* blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, BTREE_DELETE_PAGE_TARGET_BLOCK_NUM, recordstatehead);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_DELETE_PAGE_PARENT_BLOCK_NUM, blockstate);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, BTREE_DELETE_PAGE_RIGHT_BLOCK_NUM, blockstate);

    if (xlrec->leftblk != P_NONE) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_DELETE_PAGE_LEFT_BLOCK_NUM, blockstate);
    }

    /* Update metapage if needed */
    if (info == XLOG_BTREE_DELETE_PAGE_META) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, BTREE_DELETE_PAGE_META_BLOCK_NUM, blockstate);
    }

    /* forget incomplete action */

    if (info == XLOG_BTREE_DELETE_PAGE_HALF) {
        /* log incomplete action */
    }

    return recordstatehead;
}

static XLogRecParseState* btree_xlog_newroot_parse_block(XLogReaderState* record, uint32* blocknum)
{
    xl_btree_newroot* xlrec = (xl_btree_newroot*)XLogRecGetData(record);
    XLogRecParseState* recordstatehead = NULL;
    XLogRecParseState* blockstate = NULL;

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

static XLogRecParseState* btree_xlog_reuse_page_parse_block(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    xl_btree_reuse_page* xlrec = (xl_btree_reuse_page*)XLogRecGetData(record);

    *blocknum = 0;
    if (SUPPORT_HOT_STANDBY) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
        if (recordstatehead == NULL) {
            return NULL;
        }

        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->node, XLogRecGetBucketId(record));
        XLogRecSetBlockCommonState(
            record, BLOCK_DATA_INVALIDMSG_TYPE, InvalidForkNumber, InvalidBlockNumber, &rnode, recordstatehead);
        XLogRecSetInvalidMsgState(&recordstatehead->blockparse.extra_rec.blockinvalidmsg, xlrec->latestRemovedXid);
    }
    return recordstatehead;
}

XLogRecParseState* btree_redo_parse_to_block(XLogReaderState* record, uint32* blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState* recordblockstate = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
        case XLOG_BTREE_INSERT_UPPER:
        case XLOG_BTREE_INSERT_META:
            recordblockstate = btree_xlog_insert_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
        case XLOG_BTREE_SPLIT_L_ROOT:
        case XLOG_BTREE_SPLIT_R_ROOT:
            recordblockstate = btree_xlog_split_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_VACUUM:
            recordblockstate = btree_xlog_vacuum_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_DELETE:
            recordblockstate = btree_xlog_delete_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_DELETE_PAGE:
        case XLOG_BTREE_DELETE_PAGE_META:
        case XLOG_BTREE_DELETE_PAGE_HALF:
            recordblockstate = btree_xlog_delete_page_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_NEWROOT:
            recordblockstate = btree_xlog_newroot_parse_block(record, blocknum);
            break;
        case XLOG_BTREE_REUSE_PAGE:
            recordblockstate = btree_xlog_reuse_page_parse_block(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("btree_redo_parse_to_block: unknown op code %u", info)));
    }

    return recordblockstate;
}

static void btree_xlog_insert_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    Size blkdatalen;
    char* blkdata = NULL;
    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_INSERT_ORIG_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            Assert(blkdata != NULL);
            char* maindata = XLogBlockDataGetMainData(datadecode, NULL);

            btree_xlog_insert_operator_page(bufferinfo, (void*)maindata, (void*)blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_INSERT_CHILD_BLOCK_NUM) {
        /* child */
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            btree_xlog_clear_incomplete_split(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        /* meta */
        btree_restore_meta_operator_page(bufferinfo, (void*)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void btree_xlog_split_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    XLogBlockDataParse* datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_RIGHT_BLOCK_NUM) {
        /* right page */
        BlockNumber leftsib;
        BlockNumber rnext;
        Size blkdatalen;
        char* blkdata = NULL;
        char* maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        rnext = XLogBlockDataGetAuxiBlock1(datadecode);
        leftsib = XLogBlockDataGetAuxiBlock2(datadecode);

        btree_xlog_split_operator_rightpage(bufferinfo, (void*)maindata, leftsib, rnext, (void*)blkdata, blkdatalen);
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
                char* blkdata = NULL;
                char* maindata = XLogBlockDataGetMainData(datadecode, NULL);
                bool onleft = ((info == XLOG_BTREE_SPLIT_L) || (info == XLOG_BTREE_SPLIT_L_ROOT));

                blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
                btree_xlog_split_operator_leftpage(
                    bufferinfo, (void*)maindata, rightsib, onleft, (void*)blkdata, blkdatalen, NULL, 0);
            } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM) {
                /* right next */
                btree_xlog_split_operator_nextpage(bufferinfo, rightsib);
            } else {
                /* child */
                btree_xlog_clear_incomplete_split(bufferinfo);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void btree_xlog_vacuum_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char* maindata = XLogBlockDataGetMainData(datadecode, NULL);
        Size blkdatalen = 0;
        char* blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        btree_xlog_vacuum_operator_page(bufferinfo, (void*)maindata, (void*)blkdata, blkdatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void btree_xlog_delete_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size maindatalen;
        char* maindata = XLogBlockDataGetMainData(datadecode, &maindatalen);
        btree_xlog_delete_operator_page(bufferinfo, (void*)maindata, maindatalen);

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void btree_xlog_delete_page_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    XLogBlockDataParse* datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char* maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == BTREE_DELETE_PAGE_TARGET_BLOCK_NUM) {
        btree_xlog_delete_page_operator_currentpage(bufferinfo, (void*)maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else if (block_id == BTREE_DELETE_PAGE_META_BLOCK_NUM) {
        Size blkdatalen;
        char* blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        btree_restore_meta_operator_page(bufferinfo, (void*)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            if (block_id == BTREE_DELETE_PAGE_PARENT_BLOCK_NUM) {
                btree_xlog_delete_page_operator_parentpage(bufferinfo, (void*)maindata, info);
            } else if (block_id == BTREE_DELETE_PAGE_RIGHT_BLOCK_NUM) {
                btree_xlog_delete_page_operator_rightpage(bufferinfo, (void*)maindata);
            } else {
                btree_xlog_delete_page_operator_leftpage(bufferinfo, (void*)maindata);
            }
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void btree_xlog_newroot_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    Size blkdatalen;
    char* blkdata = NULL;
    BlockNumber downlink = 0;

    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

    if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_ORIG_BLOCK_NUM) {
        char* maindata = XLogBlockDataGetMainData(datadecode, NULL);
        btree_xlog_newroot_operator_page(bufferinfo, (void*)maindata, (void*)blkdata, blkdatalen, &downlink);
        MakeRedoBufferDirty(bufferinfo);
    } else if (XLogBlockDataGetBlockId(datadecode) == BTREE_NEWROOT_LEFT_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            btree_xlog_clear_incomplete_split(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        btree_restore_meta_operator_page(bufferinfo, (void*)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void btree_redo_data_block(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
        case XLOG_BTREE_INSERT_UPPER:
        case XLOG_BTREE_INSERT_META:
            btree_xlog_insert_block(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
        case XLOG_BTREE_SPLIT_L_ROOT:
        case XLOG_BTREE_SPLIT_R_ROOT:
            btree_xlog_split_block(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_VACUUM:
            btree_xlog_vacuum_block(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_DELETE:
            btree_xlog_delete_block(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_DELETE_PAGE:
        case XLOG_BTREE_DELETE_PAGE_META:
        case XLOG_BTREE_DELETE_PAGE_HALF:
            btree_xlog_delete_page_block(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_BTREE_NEWROOT:
            btree_xlog_newroot_block(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("btree_redo_block: unknown op code %u", info)));
    }
}
