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
 * hash.cpp
 *    parse hash xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/hash.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/hash_xlog.h"
#include "access/relscan.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"

#include "catalog/index.h"
#include "commands/vacuum.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

static XLogRecParseState *HashXlogInitMetaPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_INIT_META_PAGE_NUM, recordstatehead);

    return recordstatehead;
}

static XLogRecParseState *HashXlogInitBitmapPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_INIT_BITMAP_PAGE_BITMAP_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_INIT_BITMAP_PAGE_META_NUM, blockstate);

    *blocknum = 2;
    return recordstatehead;
}

static XLogRecParseState *HashXlogInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_INSERT_PAGE_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_INSERT_META_NUM, blockstate);

    *blocknum = 2;
    return recordstatehead;
}

static XLogRecParseState *HashXlogAddOvflPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    BlockNumber leftblk;
    BlockNumber rightblk;

    XLogRecGetBlockTag(record, 0, NULL, NULL, &rightblk);
    XLogRecGetBlockTag(record, 1, NULL, NULL, &leftblk);

    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_ADD_OVFL_PAGE_OVFL_NUM, recordstatehead);
    XLogRecSetAuxiBlkNumState(&recordstatehead->blockparse.extra_rec.blockdatarec, leftblk, InvalidForkNumber);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_ADD_OVFL_PAGE_LEFT_NUM, blockstate);
    XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, rightblk, InvalidForkNumber);

    *blocknum = 2;

    if (XLogRecHasBlockRef(record, XLOG_HASH_ADD_OVFL_PAGE_MAP_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, XLOG_HASH_ADD_OVFL_PAGE_MAP_NUM, blockstate);
    }

    if (XLogRecHasBlockRef(record, XLOG_HASH_ADD_OVFL_PAGE_NEWMAP_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, XLOG_HASH_ADD_OVFL_PAGE_NEWMAP_NUM, blockstate);
    }

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_ADD_OVFL_PAGE_META_NUM, blockstate);

    return recordstatehead;
}

static XLogRecParseState *HashXlogSplitAllocatePageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_ALLOCATE_PAGE_OBUK_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_ALLOCATE_PAGE_NBUK_NUM, blockstate);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_ALLOCATE_PAGE_META_NUM, blockstate);

    *blocknum = 3;
    return recordstatehead;
}

static XLogRecParseState *HashXlogSplitPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_PAGE_NUM, recordstatehead);

    *blocknum = 1;
    return recordstatehead;
}

static XLogRecParseState *HashXlogSplitCompleteParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_COMPLETE_OBUK_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, XLOG_HASH_SPLIT_COMPLETE_NBUK_NUM, blockstate);

    *blocknum = 2;
    return recordstatehead;
}

static XLogRecParseState *HashXlogMovePageContentsParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    xl_hash_move_page_contents *xldata = (xl_hash_move_page_contents *) XLogRecGetData(record);

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (xldata->is_prim_bucket_same_wrt) {
        XLogRecSetBlockDataState(record, HASH_MOVE_ADD_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecParseState *blockstate = NULL;
        XLogRecSetBlockDataState(record, HASH_MOVE_BUK_BLOCK_NUM, recordstatehead);
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }

        XLogRecSetBlockDataState(record, HASH_MOVE_ADD_BLOCK_NUM, blockstate);
        (*blocknum)++;
    }

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);

    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_MOVE_DELETE_OVFL_BLOCK_NUM, blockstate);
    (*blocknum)++;

    return recordstatehead;
}

static XLogRecParseState *HashXlogSqueezePageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;
    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) XLogRecGetData(record);

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (xldata->is_prim_bucket_same_wrt) {
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_ADD_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_BUK_BLOCK_NUM, recordstatehead);
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_ADD_BLOCK_NUM, blockstate);
        (*blocknum)++;
    }

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_SQUEEZE_INIT_OVFLBUF_BLOCK_NUM, blockstate);

    if (!xldata->is_prev_bucket_same_wrt) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_UPDATE_PREV_BLOCK_NUM, blockstate);
    }

    if (XLogRecHasBlockRef(record, HASH_SQUEEZE_UPDATE_NEXT_BLOCK_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_UPDATE_NEXT_BLOCK_NUM, blockstate);
    }

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_SQUEEZE_UPDATE_BITMAP_BLOCK_NUM, blockstate);

    if (XLogRecHasBlockRef(record, HASH_SQUEEZE_UPDATE_META_BLOCK_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HASH_SQUEEZE_UPDATE_META_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *HashXlogDeleteParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    xl_hash_delete *xldata = (xl_hash_delete *)XLogRecGetData(record);

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (xldata->is_primary_bucket_page) {
        XLogRecSetBlockDataState(record, HASH_DELETE_OVFL_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecParseState *blockstate = NULL;
        XLogRecSetBlockDataState(record, HASH_DELETE_BUK_BLOCK_NUM, recordstatehead);
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }

        XLogRecSetBlockDataState(record, HASH_DELETE_OVFL_BLOCK_NUM, blockstate);
        (*blocknum)++;
    }

    return recordstatehead;
}

static XLogRecParseState *HashXlogSplitCleanupParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_SPLIT_CLEANUP_BLOCK_NUM, recordstatehead);

    *blocknum = 1;
    return recordstatehead;
}

static XLogRecParseState *HashXlogUpdateMetaPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_UPDATE_META_BLOCK_NUM, recordstatehead);

    *blocknum = 1;
    return recordstatehead;
}

static XLogRecParseState *HashXlogVacuumOnePageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_VACUUM_PAGE_BLOCK_NUM, recordstatehead);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HASH_VACUUM_META_BLOCK_NUM, blockstate);

    *blocknum = 2;

    return recordstatehead;
}

XLogRecParseState *HashRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_HASH_INIT_META_PAGE:
            recordblockstate = HashXlogInitMetaPageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_INIT_BITMAP_PAGE:
            recordblockstate = HashXlogInitBitmapPageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_INSERT:
            recordblockstate = HashXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_HASH_ADD_OVFL_PAGE:
            recordblockstate = HashXlogAddOvflPageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
            recordblockstate = HashXlogSplitAllocatePageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_SPLIT_PAGE:
            recordblockstate = HashXlogSplitPageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_SPLIT_COMPLETE:
            recordblockstate = HashXlogSplitCompleteParseBlock(record, blocknum);
            break;
        case XLOG_HASH_MOVE_PAGE_CONTENTS:
            recordblockstate = HashXlogMovePageContentsParseBlock(record, blocknum);
            break;
        case XLOG_HASH_SQUEEZE_PAGE:
            recordblockstate = HashXlogSqueezePageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_DELETE:
            recordblockstate = HashXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_HASH_SPLIT_CLEANUP:
            recordblockstate = HashXlogSplitCleanupParseBlock(record, blocknum);
            break;
        case XLOG_HASH_UPDATE_META_PAGE:
            recordblockstate = HashXlogUpdateMetaPageParseBlock(record, blocknum);
            break;
        case XLOG_HASH_VACUUM_ONE_PAGE:
            recordblockstate = HashXlogVacuumOnePageParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("hash_redo_block: unknown op code %u", info)));
    }

    return recordblockstate;
}

void HashRedoInitMetaPageOperatorPage(RedoBufferInfo *metabuf, void *recorddata)
{
    xl_hash_init_meta_page *xlrec = (xl_hash_init_meta_page *)recorddata;
    _hash_init_metabuffer(metabuf->buf, xlrec->num_tuples, xlrec->procid, xlrec->ffactor, true);
    PageSetLSN(metabuf->pageinfo.page, metabuf->lsn);
}

void HashRedoInitBitmapPageOperatorBitmapPage(RedoBufferInfo *bitmapbuf, void *recorddata)
{
    xl_hash_init_bitmap_page *xlrec = (xl_hash_init_bitmap_page *)recorddata;
    _hash_initbitmapbuffer(bitmapbuf->buf, xlrec->bmsize, true);
    PageSetLSN(bitmapbuf->pageinfo.page, bitmapbuf->lsn);
}

void HashRedoInitBitmapPageOperatorMetaPage(RedoBufferInfo *metabuf)
{
    uint32 num_buckets;
    HashMetaPage metap;

    metap = HashPageGetMeta(metabuf->pageinfo.page);
    num_buckets = metap->hashm_maxbucket + 1;
    metap->hashm_mapp[metap->hashm_nmaps] = num_buckets + 1;
    metap->hashm_nmaps++;

    PageSetLSN(metabuf->pageinfo.page, metabuf->lsn);
}

void HashRedoInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *data, Size datalen)
{
    xl_hash_insert *xlrec = (xl_hash_insert *)recorddata;
    Page page = buffer->pageinfo.page;
    char *datapos = (char *)data;

    if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum, false, false) == InvalidOffsetNumber) {
        ereport(PANIC, (errmsg("hash_xlog_insert: failed to add item")));
    }

    PageSetLSN(page, buffer->lsn);
}

void HashRedoInsertOperatorMetaPage(RedoBufferInfo *metabuf)
{
    HashMetaPage metap;

    metap = HashPageGetMeta(metabuf->pageinfo.page);
    metap->hashm_ntuples += 1;

    PageSetLSN(metabuf->pageinfo.page, metabuf->lsn);
}

void HashRedoAddOvflPageOperatorOvflPage(RedoBufferInfo *ovflbuf, BlockNumber leftblk, void *data, Size datalen)
{
    Page ovflpage;
    HashPageOpaque ovflopaque;
    uint32 *num_bucket;

    num_bucket = (uint32 *)data;
    Assert(datalen == sizeof(uint32));
    _hash_initbuf(ovflbuf->buf, InvalidBlockNumber, *num_bucket, LH_OVERFLOW_PAGE, true);
    /* update backlink */
    ovflpage = ovflbuf->pageinfo.page;
    ovflopaque = (HashPageOpaque) PageGetSpecialPointer(ovflpage);
    ovflopaque->hasho_prevblkno = leftblk;

    PageSetLSN(ovflpage, ovflbuf->lsn);
}

void HashRedoAddOvflPageOperatorLeftPage(RedoBufferInfo *leftbuf, BlockNumber rightblk)
{
    Page leftpage;
    HashPageOpaque leftopaque;

    leftpage = leftbuf->pageinfo.page;
    leftopaque = (HashPageOpaque) PageGetSpecialPointer(leftpage);
    leftopaque->hasho_nextblkno = rightblk;

    PageSetLSN(leftpage, leftbuf->lsn);
}

void HashRedoAddOvflPageOperatorMapPage(RedoBufferInfo *mapbuf, void *data)
{
    uint32 *bitmap_page_bit = (uint32 *)data;
    Page mappage = mapbuf->pageinfo.page;
    uint32 *freep = NULL;

    freep = HashPageGetBitmap(mappage);
    SETBIT(freep, *bitmap_page_bit);

    PageSetLSN(mappage, mapbuf->lsn);
}

void HashRedoAddOvflPageOperatorNewmapPage(RedoBufferInfo *newmapbuf, void *recorddata)
{
    xl_hash_add_ovfl_page *xlrec = (xl_hash_add_ovfl_page *)recorddata;

    _hash_initbitmapbuffer(newmapbuf->buf, xlrec->bmsize, true);

    PageSetLSN(newmapbuf->pageinfo.page, newmapbuf->lsn);
}

void HashRedoAddOvflPageOperatorMetaPage(RedoBufferInfo *metabuf, void *recorddata, void *data, Size datalen)
{
    HashMetaPage metap;
    uint32 firstfree_ovflpage;
    BlockNumber *newmapblk = NULL;
    xl_hash_add_ovfl_page *xlrec = (xl_hash_add_ovfl_page *)recorddata;
    errno_t rc = EOK;

    rc = memcpy_s(&firstfree_ovflpage, sizeof(uint32), data, sizeof(uint32));
    securec_check(rc, "", "");
    metap = HashPageGetMeta(metabuf->pageinfo.page);
    metap->hashm_firstfree = firstfree_ovflpage;

    if (!xlrec->bmpage_found) {
        metap->hashm_spares[metap->hashm_ovflpoint]++;

        if (datalen > sizeof(uint32)) {
            Assert(datalen == sizeof(uint32) + sizeof(BlockNumber));

            newmapblk = (BlockNumber *)((char *)data + sizeof(uint32));
            Assert(BlockNumberIsValid(*newmapblk));

            metap->hashm_mapp[metap->hashm_nmaps] = *newmapblk;
            metap->hashm_nmaps++;
            metap->hashm_spares[metap->hashm_ovflpoint]++;
        }
    }

    PageSetLSN(metabuf->pageinfo.page, metabuf->lsn);
}

void HashRedoSplitAllocatePageOperatorObukPage(RedoBufferInfo *oldbukbuf, void *recorddata)
{
    Page oldpage;
    HashPageOpaque oldopaque;
    xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *)recorddata;

    oldpage = oldbukbuf->pageinfo.page;
    oldopaque = (HashPageOpaque) PageGetSpecialPointer(oldpage);

    oldopaque->hasho_flag = xlrec->old_bucket_flag;
    oldopaque->hasho_prevblkno = xlrec->new_bucket;

    PageSetLSN(oldpage, oldbukbuf->lsn);
}

void HashRedoSplitAllocatePageOperatorNbukPage(RedoBufferInfo *newbukbuf, void *recorddata)
{
    xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *)recorddata;

    _hash_initbuf(newbukbuf->buf, xlrec->new_bucket, xlrec->new_bucket, xlrec->new_bucket_flag, true);

    PageSetLSN(newbukbuf->pageinfo.page, newbukbuf->lsn);
}

void HashRedoSplitAllocatePageOperatorMetaPage(RedoBufferInfo *metabuf, void *recorddata, void *blkdata)
{
    HashMetaPage metap;
    char *data = (char *)blkdata;
    xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *)recorddata;

    metap = HashPageGetMeta(metabuf->pageinfo.page);
    metap->hashm_maxbucket = xlrec->new_bucket;

    if (xlrec->flags & XLH_SPLIT_META_UPDATE_MASKS) {
        uint32 lowmask;
        uint32 *highmask = NULL;
        errno_t rc = EOK;

        /* extract low and high masks. */
        rc = memcpy_s(&lowmask, sizeof(uint32), data, sizeof(uint32));
        securec_check(rc, "", "");
        highmask = (uint32 *)((char *)data + sizeof(uint32));

        /* update metapage */
        metap->hashm_lowmask = lowmask;
        metap->hashm_highmask = *highmask;

        data += sizeof(uint32) * 2;
    }

    if (xlrec->flags & XLH_SPLIT_META_UPDATE_SPLITPOINT) {
        uint32 ovflpoint;
        uint32 *ovflpages = NULL;
        errno_t rc = EOK;

        /* extract information of overflow pages. */
        rc = memcpy_s(&ovflpoint, sizeof(uint32), data, sizeof(uint32));
        securec_check(rc, "", "");
        ovflpages = (uint32 *)((char *)data + sizeof(uint32));

        /* update metapage */
        metap->hashm_spares[ovflpoint] = *ovflpages;
        metap->hashm_ovflpoint = ovflpoint;
    }

    PageSetLSN(metabuf->pageinfo.page, metabuf->lsn);
}

void HashRedoSplitCompleteOperatorObukPage(RedoBufferInfo *oldbukbuf, void *recorddata)
{
    Page oldpage;
    HashPageOpaque oldopaque;
    xl_hash_split_complete *xlrec = (xl_hash_split_complete *)recorddata;

    oldpage = oldbukbuf->pageinfo.page;
    oldopaque = (HashPageOpaque) PageGetSpecialPointer(oldpage);
    oldopaque->hasho_flag = xlrec->old_bucket_flag;

    PageSetLSN(oldpage, oldbukbuf->lsn);
}

void HashRedoSplitCompleteOperatorNbukPage(RedoBufferInfo *newbukbuf, void *recorddata)
{
    Page newpage;
    HashPageOpaque newopaque;
    xl_hash_split_complete *xlrec = (xl_hash_split_complete *)recorddata;

    newpage = newbukbuf->pageinfo.page;
    newopaque = (HashPageOpaque) PageGetSpecialPointer(newpage);
    newopaque->hasho_flag = xlrec->new_bucket_flag;

    PageSetLSN(newpage, newbukbuf->lsn);
}

void HashXlogMoveAddPageOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, void *blkdata, Size len)
{
    Page writepage = redobuffer->pageinfo.page;;
    char *begin = (char *)blkdata;
    char *data = (char *)blkdata;
    Size datalen = len;
    uint16 ninserted = 0;

    xl_hash_move_page_contents *xldata = (xl_hash_move_page_contents *) (recorddata);

    if (xldata->ntups > 0) {
        OffsetNumber *towrite = (OffsetNumber *) data;

        data += sizeof(OffsetNumber) * xldata->ntups;

        while ((Size)(data - begin) < datalen) {
            IndexTuple itup = (IndexTuple) data;
            Size itemsz;
            OffsetNumber l;

            itemsz = IndexTupleDSize(*itup);
            itemsz = MAXALIGN(itemsz);

            data += itemsz;

            l = PageAddItem(writepage, (Item) itup, itemsz, towrite[ninserted], false, false);
            if (l == InvalidOffsetNumber)
                elog(ERROR, "hash_xlog_move_page_contents: failed to add item to hash index page, size %d bytes",
                     (int) itemsz);

            ninserted++;
        }
    }

    /*
        * number of tuples inserted must be same as requested in REDO record.
        */
    Assert(ninserted == xldata->ntups);

    PageSetLSN(writepage, redobuffer->lsn);
}

void HashXlogMoveDeleteOvflPageOperatorPage(RedoBufferInfo *redobuffer, void *blkdata, Size len)
{
    Page page = redobuffer->pageinfo.page;;
    char *data = (char *)blkdata;
    Size datalen = len;

    if (datalen > 0) {
        OffsetNumber *unused;
        OffsetNumber *unend;

        unused = (OffsetNumber *) data;
        unend = (OffsetNumber *) ((char *) data + len);

        if ((unend - unused) > 0)
            PageIndexMultiDelete(page, unused, unend - unused);
    }

    PageSetLSN(page, redobuffer->lsn);
}

/* adding item to overflow buffer(writepage) from free overflowpage */
void HashXlogSqueezeAddPageOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, void *blkdata, Size len)
{
    Page writepage = redobuffer->pageinfo.page;
    char *begin = (char *)blkdata;
    char *data = (char *)blkdata;
    Size datalen = len;
    uint16 ninserted = 0;

    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) (recorddata);

    if (xldata->ntups > 0) {
        OffsetNumber *towrite = (OffsetNumber *) data;

        data += sizeof(OffsetNumber) * xldata->ntups;

        while ((Size)(data - begin) < datalen) {
            IndexTuple itup = (IndexTuple) data;
            Size itemsz;
            OffsetNumber l;

            itemsz = IndexTupleDSize(*itup);
            itemsz = MAXALIGN(itemsz);

            data += itemsz;

            l = PageAddItem(writepage, (Item) itup, itemsz, towrite[ninserted], false, false);
            if (l == InvalidOffsetNumber)
                elog(ERROR, "hash_xlog_squeeze_page: failed to add item to hash index page, size %d bytes",
                     (int) itemsz);

            ninserted++;
        }
    }

    /*
     * number of tuples inserted must be same as requested in REDO record.
     */
    Assert(ninserted == xldata->ntups);

    /*
     * if the page on which are adding tuples is a page previous to freed
     * overflow page, then update its nextblkno.
     */
    if (xldata->is_prev_bucket_same_wrt) {
        HashPageOpaque writeopaque = (HashPageOpaque) PageGetSpecialPointer(writepage);

        writeopaque->hasho_nextblkno = xldata->nextblkno;
    }

    PageSetLSN(writepage, redobuffer->lsn);
}

/* initializing free overflow page */
void HashXlogSqueezeInitOvflbufOperatorPage(RedoBufferInfo *redobuffer, void *recorddata)
{
    Page ovflpage;
    HashPageOpaque ovflopaque;

    ovflpage = redobuffer->pageinfo.page;

    _hash_pageinit(ovflpage, BufferGetPageSize(redobuffer->buf));

    ovflopaque = (HashPageOpaque) PageGetSpecialPointer(ovflpage);

    ovflopaque->hasho_prevblkno = InvalidBlockNumber;
    ovflopaque->hasho_nextblkno = InvalidBlockNumber;
    ovflopaque->hasho_bucket = InvalidBucket;
    ovflopaque->hasho_flag = LH_UNUSED_PAGE;
    ovflopaque->hasho_page_id = HASHO_PAGE_ID;

    PageSetLSN(ovflpage, redobuffer->lsn);
}

void HashXlogSqueezeUpdatePrevPageOperatorPage(RedoBufferInfo *redobuffer, void *recorddata)
{
    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) (recorddata);

    Page prevpage = redobuffer->pageinfo.page;
    HashPageOpaque prevopaque = (HashPageOpaque) PageGetSpecialPointer(prevpage);

    prevopaque->hasho_nextblkno = xldata->nextblkno;

    PageSetLSN(prevpage, redobuffer->lsn);
}

void HashXlogSqueezeUpdateNextPageOperatorPage(RedoBufferInfo *redobuffer, void *recorddata)
{
    xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) (recorddata);

    Page nextpage = redobuffer->pageinfo.page;
    HashPageOpaque nextopaque = (HashPageOpaque) PageGetSpecialPointer(nextpage);

    nextopaque->hasho_prevblkno = xldata->prevblkno;

    PageSetLSN(nextpage, redobuffer->lsn);
}

void HashXlogSqueezeUpdateBitmapOperatorPage(RedoBufferInfo *redobuffer, void *blkdata)
{
    Page mappage = redobuffer->pageinfo.page;
    uint32 *freep = NULL;
    char *data = (char *)blkdata;
    uint32 *bitmap_page_bit;

    freep = HashPageGetBitmap(mappage);

    bitmap_page_bit = (uint32 *) data;

    CLRBIT(freep, *bitmap_page_bit);

    PageSetLSN(mappage, redobuffer->lsn);
}

void HashXlogSqueezeUpdateMateOperatorPage(RedoBufferInfo *redobuffer, void *blkdata)
{
    HashMetaPage metap;
    Page page = redobuffer->pageinfo.page;
    char *data = (char *)blkdata;
    uint32 *firstfree_ovflpage;

    firstfree_ovflpage = (uint32 *) data;

    metap = HashPageGetMeta(page);
    metap->hashm_firstfree = *firstfree_ovflpage;

    PageSetLSN(page, redobuffer->lsn);
}

void HashXlogDeleteBlockOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, void *blkdata, Size len)
{
    xl_hash_delete *xldata = (xl_hash_delete *)(recorddata);

    Page page = redobuffer->pageinfo.page;
    char *datapos = (char *)blkdata;

    if (len > 0) {
        OffsetNumber *unused;
        OffsetNumber *unend;

        unused = (OffsetNumber *) datapos;
        unend = (OffsetNumber *) ((char *) datapos + len);

        if ((unend - unused) > 0) {
            PageIndexMultiDelete(page, unused, unend - unused);
        }
    }

    /*
     * Mark the page as not containing any LP_DEAD items only if
     * clear_dead_marking flag is set to true. See comments in
     * hashbucketcleanup() for details.
     */
    if (xldata->clear_dead_marking) {
        HashPageOpaque pageopaque;

        pageopaque = (HashPageOpaque) PageGetSpecialPointer(page);
        pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;
    }

    PageSetLSN(page, redobuffer->lsn);
}

void HashXlogSplitCleanupOperatorPage(RedoBufferInfo *redobuffer)
{
    Page page;
    HashPageOpaque bucket_opaque;

    page = redobuffer->pageinfo.page;
    bucket_opaque = (HashPageOpaque) PageGetSpecialPointer(page);

    /* cleanup flag for finished split */
    bucket_opaque->hasho_flag &= ~LH_BUCKET_NEEDS_SPLIT_CLEANUP;

    PageSetLSN(page, redobuffer->lsn);
}

void HashXlogUpdateMetaOperatorPage(RedoBufferInfo *redobuffer, void *recorddata)
{
    Page page;
    HashMetaPage metap;
    xl_hash_update_meta_page *xldata = (xl_hash_update_meta_page *) (recorddata);

    page = redobuffer->pageinfo.page;
    metap = HashPageGetMeta(page);

    metap->hashm_ntuples = xldata->ntuples;

    PageSetLSN(page, redobuffer->lsn);

}

void HashXlogVacuumOnePageOperatorPage(RedoBufferInfo *redobuffer, void *recorddata, Size len)
{
    Page page = redobuffer->pageinfo.page;
    xl_hash_vacuum_one_page *xldata;
    HashPageOpaque pageopaque;

    xldata = (xl_hash_vacuum_one_page *) (recorddata);

    if (len > SizeOfHashVacuumOnePage) {
        OffsetNumber *unused;

        unused = (OffsetNumber *) ((char *) xldata + SizeOfHashVacuumOnePage);

        PageIndexMultiDelete(page, unused, xldata->ntuples);
    }

    /*
     * Mark the page as not containing any LP_DEAD items. See comments in
     * _hash_vacuum_one_page() for details.
     */
    pageopaque = (HashPageOpaque) PageGetSpecialPointer(page);
    pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;

    PageSetLSN(page, redobuffer->lsn);
}

void HashXlogVacuumMateOperatorPage(RedoBufferInfo *redobuffer, void *recorddata)
{
    Page metapage;
    HashMetaPage metap;
    xl_hash_vacuum_one_page *xldata;
    xldata = (xl_hash_vacuum_one_page *) (recorddata);

    metapage = redobuffer->pageinfo.page;
    metap = HashPageGetMeta(metapage);

    metap->hashm_ntuples -= xldata->ntuples;

    PageSetLSN(metapage, redobuffer->lsn);
}

static void HashXlogInitMetaPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                      RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_INIT_META_PAGE_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        HashRedoInitMetaPageOperatorPage(bufferinfo, maindata);
        MakeRedoBufferDirty(bufferinfo);
        if (blockhead->forknum == INIT_FORKNUM) {
            FlushOneBuffer(bufferinfo->buf);
        }
    }
}

static void HashXlogInitBitmapPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                        RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    bool modifypage = false;
    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_INIT_BITMAP_PAGE_BITMAP_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        HashRedoInitBitmapPageOperatorBitmapPage(bufferinfo, maindata);
        MakeRedoBufferDirty(bufferinfo);
        modifypage = true;
    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashRedoInitBitmapPageOperatorMetaPage(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
            modifypage = true;
        }
    }

    if (blockhead->forknum == INIT_FORKNUM && modifypage) {
        FlushOneBuffer(bufferinfo->buf);
    }
}

static void HashXlogInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action != BLK_NEEDS_REDO) {
        return;
    }

    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_INSERT_PAGE_NUM) {
        Size blkdatalen;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

        HashRedoInsertOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
    } else {
        HashRedoInsertOperatorMetaPage(bufferinfo);
    }
    MakeRedoBufferDirty(bufferinfo);
}

static void HashXlogAddOvflPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                     RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_ADD_OVFL_PAGE_OVFL_NUM) {
        Size blkdatalen;
        char *blkdata = NULL;
        BlockNumber leftblk;
        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        leftblk = XLogBlockDataGetAuxiBlock1(datadecode);

        HashRedoAddOvflPageOperatorOvflPage(bufferinfo, leftblk, blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    } else if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_ADD_OVFL_PAGE_LEFT_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            BlockNumber rightblk = XLogBlockDataGetAuxiBlock1(datadecode);
            HashRedoAddOvflPageOperatorLeftPage(bufferinfo, rightblk);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_ADD_OVFL_PAGE_MAP_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            char *blkdata = XLogBlockDataGetBlockData(datadecode, NULL);
            HashRedoAddOvflPageOperatorMapPage(bufferinfo, blkdata);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_ADD_OVFL_PAGE_NEWMAP_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        HashRedoAddOvflPageOperatorNewmapPage(bufferinfo, maindata);
        MakeRedoBufferDirty(bufferinfo);

    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            Size blkdatalen;
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
            char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

            HashRedoAddOvflPageOperatorMetaPage(bufferinfo, maindata, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void HashXlogSplitAllocatePageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                           RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_SPLIT_ALLOCATE_PAGE_OBUK_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
            HashRedoSplitAllocatePageOperatorObukPage(bufferinfo, maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_SPLIT_ALLOCATE_PAGE_NBUK_NUM) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        HashRedoSplitAllocatePageOperatorNbukPage(bufferinfo, maindata);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
            char *blkdata = XLogBlockDataGetBlockData(datadecode, NULL);
            HashRedoSplitAllocatePageOperatorMetaPage(bufferinfo, maindata, blkdata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void HashXlogSplitPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                   RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action != BLK_RESTORED) {
        ereport(ERROR, (errmsg("Hash split record did not contain a full-page image")));
    }
    MakeRedoBufferDirty(bufferinfo);
}

static void HashXlogSplitCompleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                       RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action != BLK_NEEDS_REDO && action != BLK_RESTORED) {
        return;
    }

    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
    if (XLogBlockDataGetBlockId(datadecode) == XLOG_HASH_SPLIT_COMPLETE_OBUK_NUM) {
        HashRedoSplitCompleteOperatorObukPage(bufferinfo, maindata);
    } else {
        HashRedoSplitCompleteOperatorNbukPage(bufferinfo, maindata);
    }
    MakeRedoBufferDirty(bufferinfo);
}

static void HashXlogMovePageContentsBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                          RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    Size blkdatalen;
    char *blkdata = NULL;
    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == HASH_MOVE_BUK_BLOCK_NUM) {
        PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
    }

    if (block_id == HASH_MOVE_ADD_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogMoveAddPageOperatorPage(bufferinfo, maindata, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_MOVE_DELETE_OVFL_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogMoveDeleteOvflPageOperatorPage(bufferinfo, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void HashXlogSqueezePageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                     RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    Size blkdatalen;
    char *blkdata = NULL;
    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    if (block_id == HASH_SQUEEZE_BUK_BLOCK_NUM) {
        PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
    }

    if (block_id == HASH_SQUEEZE_ADD_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogSqueezeAddPageOperatorPage(bufferinfo, maindata, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_SQUEEZE_INIT_OVFLBUF_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogSqueezeInitOvflbufOperatorPage(bufferinfo, maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_SQUEEZE_UPDATE_PREV_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) (maindata);
        if (!xldata->is_prev_bucket_same_wrt && action == BLK_NEEDS_REDO) {
            HashXlogSqueezeUpdatePrevPageOperatorPage(bufferinfo, maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_SQUEEZE_UPDATE_NEXT_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogSqueezeUpdateNextPageOperatorPage(bufferinfo, maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_SQUEEZE_UPDATE_BITMAP_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogSqueezeUpdateBitmapOperatorPage(bufferinfo, blkdata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }

    if (block_id == HASH_SQUEEZE_UPDATE_META_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogSqueezeUpdateMateOperatorPage(bufferinfo, blkdata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void HashXlogDeleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    Size blkdatalen;
    char *blkdata = NULL;
    blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);

    if (block_id == HASH_DELETE_OVFL_BLOCK_NUM) {
        if (action == BLK_NEEDS_REDO) {
            HashXlogDeleteBlockOperatorPage(bufferinfo, maindata, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        if (action == BLK_NEEDS_REDO) {
            PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

static void HashXlogSplitCleanupBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                      RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        HashXlogSplitCleanupOperatorPage(bufferinfo);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HashXlogUpdateMetaPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                        RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        HashXlogUpdateMetaOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HashXlogVacuumOnePageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                       RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    uint8 block_id = XLogBlockDataGetBlockId(datadecode);
    Size maindatalen;
    char *maindata = XLogBlockDataGetMainData(datadecode, &maindatalen);

    if (block_id == HASH_VACUUM_PAGE_BLOCK_NUM) {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogVacuumOnePageOperatorPage(bufferinfo, (void *)maindata, maindatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        XLogRedoAction action;
        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            HashXlogVacuumMateOperatorPage(bufferinfo, (void *)maindata);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

void HashRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_HASH_INIT_META_PAGE:
            HashXlogInitMetaPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_INIT_BITMAP_PAGE:
            HashXlogInitBitmapPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_INSERT:
            HashXlogInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_ADD_OVFL_PAGE:
            HashXlogAddOvflPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
            HashXlogSplitAllocatePageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_SPLIT_PAGE:
            HashXlogSplitPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_SPLIT_COMPLETE:
            HashXlogSplitCompleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_MOVE_PAGE_CONTENTS:
            HashXlogMovePageContentsBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_SQUEEZE_PAGE:
            HashXlogSqueezePageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_DELETE:
            HashXlogDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_SPLIT_CLEANUP:
            HashXlogSplitCleanupBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_UPDATE_META_PAGE:
            HashXlogUpdateMetaPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HASH_VACUUM_ONE_PAGE:
            HashXlogVacuumOnePageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("hash_redo_block: unknown op code %u", info)));
    }
}
