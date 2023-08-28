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
 * gistxlog.cpp
 *    parse gist xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/gistxlog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gist_private.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "utils/memutils.h"

typedef enum {
    GIST_UPDATE_ORIG_BLOCK_NUM = 0,
    GIST_UPDATE_LEFT_CHILD_BOCK_NUM,
} XLogGistPageUpdateEnum;

typedef enum {
    GIST_SPLIT_FOLLOW_WRITE_BLOCK_NUM = 0,
} XLogGistPageSplitEnum;

typedef enum {
    GIST_CREATE_INDEX_BLOCK_NUM = 0,
} XLogGistCreatIndexEnum;

void GistRedoClearFollowRightOperatorPage(RedoBufferInfo *buffer)
{
    Page page = buffer->pageinfo.page;

    GistPageGetOpaque(page)->nsn = buffer->lsn;
    GistClearFollowRight(page);
    PageSetLSN(page, buffer->lsn);
}

void GistRedoPageUpdateOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size datalen)
{
    gistxlogPageUpdate *xldata = (gistxlogPageUpdate *)recorddata;
    char *begin = NULL;
    char *data = NULL;
    int ninserted = 0;

    Page page = buffer->pageinfo.page;
    BlockNumber blkno = buffer->blockinfo.blkno;

    data = begin = (char *)blkdata;

    /* Delete old tuples */
    if (xldata->ntodelete > 0) {
        int i;
        OffsetNumber *todelete = (OffsetNumber *)data;

        data += sizeof(OffsetNumber) * xldata->ntodelete;

        for (i = 0; i < xldata->ntodelete; i++)
            PageIndexTupleDelete(page, todelete[i]);
        if (GistPageIsLeaf(page))
            GistMarkTuplesDeleted(page);
    }

    /* add tuples */
    if (data - begin < (int)datalen) {
        OffsetNumber off = (PageIsEmpty(page)) ? FirstOffsetNumber : OffsetNumberNext(PageGetMaxOffsetNumber(page));

        while (data - begin < (int)datalen) {
            IndexTuple itup = (IndexTuple)data;
            Size sz = IndexTupleSize(itup);
            OffsetNumber l;

            data += sz;

            l = PageAddItem(page, (Item)itup, sz, off, false, false);
            if (l == InvalidOffsetNumber)
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("failed to add item to GiST index page, size %d bytes", (int)sz)));
            off++;
            ninserted++;
        }

    } else {
        /*
         * special case: leafpage, nothing to insert, nothing to delete, then
         * vacuum marks page
         */
        if (GistPageIsLeaf(page) && xldata->ntodelete == 0)
            GistClearTuplesDeleted(page);
    }

    if (!GistPageIsLeaf(page) && PageGetMaxOffsetNumber(page) == InvalidOffsetNumber && blkno == GIST_ROOT_BLKNO) {
        /*
         * all links on non-leaf root page was deleted by vacuum full, so root
         * page becomes a leaf
         */
        GistPageSetLeaf(page);
    }

    Assert(ninserted == xldata->ntoinsert);

    PageSetLSN(page, buffer->lsn);
}

void GistRedoPageSplitOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *data, Size datalen, bool markflag,
                                   BlockNumber rightlink)
{
    gistxlogPageSplit *xldata = (gistxlogPageSplit *)recorddata;
    Page page = buffer->pageinfo.page;
    int num;
    IndexTuple *tuples = NULL;
    int flags;

    tuples = decodePageSplitRecord((char *)data, datalen, &num);

    /* ok, clear buffer */
    if (xldata->origleaf && buffer->blockinfo.blkno != GIST_ROOT_BLKNO)
        flags = F_LEAF;
    else
        flags = 0;
    GISTInitPage(page, flags, buffer->pageinfo.pagesize);

    /* and fill it */
    gistfillbuffer(page, tuples, num, FirstOffsetNumber);

    if (buffer->blockinfo.blkno == GIST_ROOT_BLKNO) {
        GistPageGetOpaque(page)->rightlink = InvalidBlockNumber;
        GistPageGetOpaque(page)->nsn = xldata->orignsn;
        GistClearFollowRight(page);
    } else {
        GistPageGetOpaque(page)->rightlink = rightlink;
        GistPageGetOpaque(page)->nsn = xldata->orignsn;
        if (markflag)
            GistMarkFollowRight(page);
        else
            GistClearFollowRight(page);
    }

    PageSetLSN(page, buffer->lsn);
}

void GistRedoCreateIndexOperatorPage(RedoBufferInfo *buffer)
{
    Assert(buffer->blockinfo.blkno == GIST_ROOT_BLKNO);
    Page page = buffer->pageinfo.page;

    GISTInitPage(page, F_LEAF, buffer->pageinfo.pagesize);

    PageSetLSN(page, buffer->lsn);
}

static XLogRecParseState *GistXlogUpdateParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, GIST_UPDATE_ORIG_BLOCK_NUM, recordstatehead);

    if (XLogRecHasBlockRef(record, GIST_UPDATE_LEFT_CHILD_BOCK_NUM)) {
        XLogRecParseState *blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);

        XLogRecSetBlockDataState(record, GIST_UPDATE_LEFT_CHILD_BOCK_NUM, blockstate);
        *blocknum += 1;
    }
    return recordstatehead;
}

static XLogRecParseState *GistXlogPageSplitParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    gistxlogPageSplit *xldata = (gistxlogPageSplit *)XLogRecGetData(record);
    *blocknum = xldata->npage;
    bool isrootsplit = false;
    for (uint16 i = 0; i < xldata->npage; i++) {
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (recordstatehead == NULL) {
            recordstatehead = blockstate;
        }
        XLogRecSetBlockDataState(record, i + 1, blockstate);

        BlockNumber blkno;
        XLogRecGetBlockTag(record, i + 1, NULL, NULL, &blkno);
        if (blkno == GIST_ROOT_BLKNO) {
            XLogRecSetBlockDataState(record, i + 1, blockstate, BLOCK_DATA_MAIN_DATA_TYPE, true);
            XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, InvalidForkNumber,
                                      InvalidForkNumber);
            isrootsplit = true;
        }
        if (blkno != GIST_ROOT_BLKNO) {
            XLogRecSetBlockDataState(record, i + 1, blockstate);
            uint32 flag;
            if ((i < xldata->npage - 1) && !isrootsplit && xldata->markfollowright)
                flag = 1;
            else
                flag = 0;

            if (i < xldata->npage - 1) {
                BlockNumber nextblkno = InvalidBlockNumber;
                XLogRecGetBlockTag(record, i + 2, NULL, NULL, &nextblkno);
                XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, nextblkno, flag);
            } else
                XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, xldata->origrlink, flag);
        }
    }

    if (XLogRecHasBlockRef(record, GIST_SPLIT_FOLLOW_WRITE_BLOCK_NUM)) {
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, GIST_SPLIT_FOLLOW_WRITE_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }
    return recordstatehead;
}

static XLogRecParseState *GistXlogCreateIndexParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, GIST_CREATE_INDEX_BLOCK_NUM, recordstatehead);
    *blocknum = 1;
    return recordstatehead;
}

XLogRecParseState *GistRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    *blocknum = 0;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    switch (info) {
        case XLOG_GIST_PAGE_UPDATE:
            recordblockstate = GistXlogUpdateParseBlock(record, blocknum);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            recordblockstate = GistXlogPageSplitParseBlock(record, blocknum);
            break;
        case XLOG_GIST_CREATE_INDEX:
            recordblockstate = GistXlogCreateIndexParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("gist parse: unknown op code %u", info)));
    }

    return recordblockstate;
}

void GistPageUpdateRedoBlock(XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == GIST_UPDATE_ORIG_BLOCK_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            Size blkdatalen = 0;
            char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
            GistRedoPageUpdateOperatorPage(bufferinfo, datadecode->main_data, blkdata, blkdatalen);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
            GistRedoClearFollowRightOperatorPage(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    }
}

void GistPageSplitRedoBlock(XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == GIST_SPLIT_FOLLOW_WRITE_BLOCK_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
            GistRedoClearFollowRightOperatorPage(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        Size blkdatalen = 0;
        char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        BlockNumber nextblkno = XLogBlockDataGetAuxiBlock1(datadecode);
        bool markflag = (XLogBlockDataGetAuxiBlock2(datadecode) > 0);
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        GistRedoPageSplitOperatorPage(bufferinfo, (void *)maindata, blkdata, blkdatalen, markflag, nextblkno);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GistCreateIndexRedoBlock(XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    GistRedoCreateIndexOperatorPage(bufferinfo);
    MakeRedoBufferDirty(bufferinfo);
}

void GistRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    MemoryContext oldCtx = MemoryContextSwitchTo(t_thrd.xlog_cxt.gist_opCtx);
    switch (info) {
        case XLOG_GIST_PAGE_UPDATE:
            GistPageUpdateRedoBlock(blockdatarec, bufferinfo);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            GistPageSplitRedoBlock(blockdatarec, bufferinfo);
            break;
        case XLOG_GIST_CREATE_INDEX:
            GistCreateIndexRedoBlock(blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("gist redo: unknown op code %u", info)));
    }
    (void)MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(t_thrd.xlog_cxt.gist_opCtx);
}
