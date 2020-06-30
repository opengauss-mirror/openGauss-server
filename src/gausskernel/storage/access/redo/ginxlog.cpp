/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
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
 * ginxlog.cpp
 *    parse gin xlog
 *
 * IDENTIFICATION
 *
 * src/gausskernel/storage/access/redo/ginxlog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gin_private.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "utils/memutils.h"

typedef enum {
    GIN_CREATE_INDEX_META_BLOCK_NUM = 0,
    GIN_CREATE_INDEX_ROOT_BLOCK_NUM,
} XLogGinCreateIndexEnum;

typedef enum {
    GIN_CREATE_P_TREE_BLOCK_NUM = 0,
} XLogGinCreatePTreeEnum;

typedef enum {
    GIN_INSERT_BLOCK_NUM = 0,
    GIN_INSERT_CLEAN_IMCOMPELTE_BLOCK_NUM,
} XLogGinInsertEnum;

typedef enum {
    GIN_SPLIT_LEFT_BLOCK_NUM = 0,
    GIN_SPLIT_RIGHT_BLOCK_NUM,
    GIN_SPLIT_ROOT_BLOCK_NUM,
    GIN_SPLIT_CLEAN_IMCOMPELTE_BLOCK_NUM,
} XLogGinSplitEnum;

typedef enum {
    GIN_VACUUM_PAGE_BLOCK_NUM = 0,
} XLogGinVacuumPageEnum;

typedef enum {
    GIN_VACUUM_DATA_LEAF_PAGE_BLOCK_NUM = 0,
} XLogGinVacuumDataLeafPageEnum;

typedef enum {
    GIN_DELETE_D_PAGE_BLOCK_NUM = 0,
    GIN_DELETE_P_PAGE_BLOCK_NUM,
    GIN_DELETE_L_PAGE_BLOCK_NUM,
} XLogGinDeletePageEnum;

typedef enum {
    GIN_META_PAGE_BLOCK_NUM = 0,
    GIN_TAIL_PAGE_BLOCK_NUM,
} XLogGinUpdataMetaPageEnum;

typedef enum {
    GIN_INSERT_LIST_PAGE_BLOCK_NUM = 0,
} XLogGinInsertListPageEnum;

typedef enum {
    GIN_DELETE_LIST_META_PAGE_BLOCK_NUM = 0,
} XLogGinDeleteListPageEnum;

static XLogRecParseState* GinXlogCreatePTreeParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_CREATE_P_TREE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;
    return recordstatehead;
}

static XLogRecParseState* GinXlogCreateIndexParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_CREATE_INDEX_META_BLOCK_NUM, recordstatehead);

    XLogRecParseState* blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, GIN_CREATE_INDEX_ROOT_BLOCK_NUM, blockstate);

    *blocknum = 2;
    return recordstatehead;
}

static XLogRecParseState* GinXlogInsertParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_INSERT_BLOCK_NUM, recordstatehead);
    *blocknum = 1;
    ginxlogInsert* data = (ginxlogInsert*)XLogRecGetData(record);
    bool isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;

    if (!isLeaf) {
        XLogRecParseState* blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, GIN_INSERT_CLEAN_IMCOMPELTE_BLOCK_NUM, blockstate);

        ++(*blocknum);
    }

    return recordstatehead;
}

static XLogRecParseState* GinXlogSplitParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_SPLIT_LEFT_BLOCK_NUM, recordstatehead);

    XLogRecParseState* blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, GIN_SPLIT_RIGHT_BLOCK_NUM, blockstate);

    *blocknum = 2;

    ginxlogSplit* data = (ginxlogSplit*)XLogRecGetData(record);
    bool isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
    bool isRoot = (data->flags & GIN_SPLIT_ROOT) != 0;

    if (isRoot) {
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, GIN_SPLIT_ROOT_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    if (!isLeaf) {
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, GIN_SPLIT_CLEAN_IMCOMPELTE_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    return recordstatehead;
}

static XLogRecParseState* GinXlogVacuumPageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_VACUUM_PAGE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    return recordstatehead;
}

static XLogRecParseState* GinXlogVacuumDataLeafPageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_VACUUM_DATA_LEAF_PAGE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    return recordstatehead;
}

static XLogRecParseState* GinXlogDeleteParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_DELETE_D_PAGE_BLOCK_NUM, recordstatehead);

    XLogRecParseState* blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, GIN_DELETE_P_PAGE_BLOCK_NUM, blockstate);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, GIN_DELETE_L_PAGE_BLOCK_NUM, blockstate);

    *blocknum = 3;
    return recordstatehead;
}

static XLogRecParseState* GinXlogUpdateMetaPageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_META_PAGE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    ginxlogUpdateMeta* data = (ginxlogUpdateMeta*)XLogRecGetData(record);
    if ((data->ntuples > 0) || (data->prevTail != InvalidBlockNumber)) {
        XLogRecParseState* blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, GIN_TAIL_PAGE_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    return recordstatehead;
}

static XLogRecParseState* GinXlogInsertListPageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_INSERT_LIST_PAGE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;
    return recordstatehead;
}

static XLogRecParseState* GinXlogDeleteListPageParseBlock(XLogReaderState* record, uint32* blocknum)
{
    XLogRecParseState* recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, GIN_DELETE_LIST_META_PAGE_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    ginxlogDeleteListPages* data = (ginxlogDeleteListPages*)XLogRecGetData(record);

    for (int32 i = 0; i < data->ndeleted; i++) {
        XLogRecParseState* blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, i + 1, blockstate);
        ++(*blocknum);
    }

    return recordstatehead;
}

XLogRecParseState* GinRedoParseToBlock(XLogReaderState* record, uint32* blocknum)
{
    *blocknum = 0;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState* recordblockstate = NULL;

    switch (info) {
        case XLOG_GIN_CREATE_INDEX:
            recordblockstate = GinXlogCreateIndexParseBlock(record, blocknum);
            break;
        case XLOG_GIN_CREATE_PTREE:
            recordblockstate = GinXlogCreatePTreeParseBlock(record, blocknum);
            break;
        case XLOG_GIN_INSERT:
            recordblockstate = GinXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_GIN_SPLIT:
            recordblockstate = GinXlogSplitParseBlock(record, blocknum);
            break;
        case XLOG_GIN_VACUUM_PAGE:
            recordblockstate = GinXlogVacuumPageParseBlock(record, blocknum);
            break;
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            recordblockstate = GinXlogVacuumDataLeafPageParseBlock(record, blocknum);
            break;
        case XLOG_GIN_DELETE_PAGE:
            recordblockstate = GinXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_GIN_UPDATE_META_PAGE:
            recordblockstate = GinXlogUpdateMetaPageParseBlock(record, blocknum);
            break;
        case XLOG_GIN_INSERT_LISTPAGE:
            recordblockstate = GinXlogInsertListPageParseBlock(record, blocknum);
            break;
        case XLOG_GIN_DELETE_LISTPAGE:
            recordblockstate = GinXlogDeleteListPageParseBlock(record, blocknum);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("GinRedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

void ginRedoCreateIndexOperatorMetaPage(RedoBufferInfo* MetaBuffer)
{
    Assert(MetaBuffer->blockinfo.blkno == GIN_METAPAGE_BLKNO);
    Page page = MetaBuffer->pageinfo.page;
    GinInitMetaPage(page, MetaBuffer->pageinfo.pagesize);
    PageSetLSN(page, MetaBuffer->lsn);
}

void ginRedoCreateIndexOperatorRootPage(RedoBufferInfo* RootBuffer)
{
    Assert(RootBuffer->blockinfo.blkno == GIN_ROOT_BLKNO);
    Page page = RootBuffer->pageinfo.page;

    GinInitPage(page, GIN_LEAF, RootBuffer->pageinfo.pagesize);

    PageSetLSN(page, RootBuffer->lsn);
}

void ginRedoCreatePTreeOperatorPage(RedoBufferInfo* buffer, void* recordData)
{
    ginxlogCreatePostingTree* data = (ginxlogCreatePostingTree*)recordData;
    char* ptr = NULL;
    Page page;
    errno_t ret = EOK;

    page = buffer->pageinfo.page;

    GinInitPage(page, GIN_DATA | GIN_LEAF | GIN_COMPRESSED, buffer->pageinfo.pagesize);

    ptr = (char*)recordData + sizeof(ginxlogCreatePostingTree);

    /* Place page data */
    ret = memcpy_s(GinDataLeafPageGetPostingList(page),
        BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData)),
        ptr,
        data->size);
    securec_check(ret, "\0", "\0");

    GinDataPageSetDataSize(page, data->size);

    PageSetLSN(page, buffer->lsn);
}

void ginRedoClearIncompleteSplitOperatorPage(RedoBufferInfo* buffer)
{
    Page page = buffer->pageinfo.page;
    GinPageGetOpaque(page)->flags &= ~GIN_INCOMPLETE_SPLIT;
    PageSetLSN(page, buffer->lsn);
}

void ginRedoVacuumDataOperatorLeafPage(RedoBufferInfo* buffer, void* recorddata)
{
    Page page = buffer->pageinfo.page;
    ginxlogVacuumDataLeafPage* xlrec = NULL;

    xlrec = (ginxlogVacuumDataLeafPage*)recorddata;

    Assert(GinPageIsLeaf(page));
    Assert(GinPageIsData(page));

    ginRedoRecompress(page, &xlrec->data);
    PageSetLSN(page, buffer->lsn);
}

void ginRedoDeletePageOperatorCurPage(RedoBufferInfo* dbuffer)
{
    Page page = dbuffer->pageinfo.page;

    Assert(GinPageIsData(page));
    GinPageGetOpaque(page)->flags = GIN_DELETED;
    PageSetLSN(page, dbuffer->lsn);
}

void ginRedoDeletePageOperatorParentPage(RedoBufferInfo* pbuffer, void* recorddata)
{
    Page page = pbuffer->pageinfo.page;

    ginxlogDeletePage* deletedata = (ginxlogDeletePage*)recorddata;

    Assert(GinPageIsData(page));
    Assert(!GinPageIsLeaf(page));
    GinPageDeletePostingItem(page, deletedata->parentOffset);
    PageSetLSN(page, pbuffer->lsn);
}

void ginRedoDeletePageOperatorLeftPage(RedoBufferInfo* lbuffer, void* recorddata)
{
    Page page = lbuffer->pageinfo.page;
    Assert(GinPageIsData(page));
    ginxlogDeletePage* deletedata = (ginxlogDeletePage*)recorddata;
    GinPageGetOpaque(page)->rightlink = deletedata->rightLink;
    PageSetLSN(page, lbuffer->lsn);
}

void ginRedoUpdateOperatorMetapage(RedoBufferInfo* metabuffer, void* recorddata)
{
    errno_t ret = EOK;

    Assert(metabuffer->blockinfo.blkno == GIN_METAPAGE_BLKNO);

    Page metapage = metabuffer->pageinfo.page;
    ginxlogUpdateMeta* data = (ginxlogUpdateMeta*)recorddata;
    GinInitPage(metapage, GIN_META, BufferGetPageSize(metabuffer->buf));
    ret = memcpy_s(GinPageGetMeta(metapage), sizeof(GinMetaPageData), &data->metadata, sizeof(GinMetaPageData));
    securec_check(ret, "", "");
    PageSetLSN(metapage, metabuffer->lsn);
}

void ginRedoUpdateOperatorTailPage(RedoBufferInfo* buffer, void* payload, Size totaltupsize, int32 ntuples)
{
    Page page = buffer->pageinfo.page;
    OffsetNumber off;
    int i;
    Size tupsize;
    IndexTuple tuples;

    tuples = (IndexTuple)payload;
    if (PageIsEmpty(page))
        off = FirstOffsetNumber;
    else
        off = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    for (i = 0; i < ntuples; i++) {
        tupsize = IndexTupleSize(tuples);

        if (PageAddItem(page, (Item)tuples, tupsize, off, false, false) == InvalidOffsetNumber)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add item to index page")));

        tuples = (IndexTuple)(((char*)tuples) + tupsize);

        off++;
    }
    Assert((char*)payload + totaltupsize == (char*)tuples);

    /*
     * Increase counter of heap tuples
     */
    GinPageGetOpaque(page)->maxoff++;

    PageSetLSN(page, buffer->lsn);
}

void ginRedoInsertListPageOperatorPage(RedoBufferInfo* buffer, void* recorddata, void* payload, Size totaltupsize)
{
    ginxlogInsertListPage* data = (ginxlogInsertListPage*)recorddata;
    Page page = buffer->pageinfo.page;
    IndexTuple tuples;
    Size tupsize;
    OffsetNumber l, off = FirstOffsetNumber;
    int i;

    GinInitPage(page, GIN_LIST, buffer->pageinfo.pagesize);
    GinPageGetOpaque(page)->rightlink = data->rightlink;
    if (data->rightlink == InvalidBlockNumber) {
        /* tail of sublist */
        GinPageSetFullRow(page);
        GinPageGetOpaque(page)->maxoff = 1;
    } else {
        GinPageGetOpaque(page)->maxoff = 0;
    }

    tuples = (IndexTuple)payload;
    for (i = 0; i < data->ntuples; i++) {
        tupsize = IndexTupleSize(tuples);

        l = PageAddItem(page, (Item)tuples, tupsize, off, false, false);

        if (l == InvalidOffsetNumber)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add item to index page")));

        tuples = (IndexTuple)(((char*)tuples) + tupsize);
        off++;
    }
    Assert((char*)tuples == (char*)payload + totaltupsize);

    PageSetLSN(page, buffer->lsn);
}

void ginRedoDeleteListPagesOperatorPage(RedoBufferInfo* metabuffer, void* recorddata)
{
    errno_t ret = EOK;
    Assert(metabuffer->blockinfo.blkno == GIN_METAPAGE_BLKNO);
    Page metapage = metabuffer->pageinfo.page;

    GinInitPage(metapage, GIN_META, metabuffer->pageinfo.pagesize);

    ret = memcpy_s(GinPageGetMeta(metapage), sizeof(GinMetaPageData), recorddata, sizeof(GinMetaPageData));
    securec_check(ret, "", "");
    PageSetLSN(metapage, metabuffer->lsn);
}

void ginRedoDeleteListPagesMarkDelete(RedoBufferInfo* buffer)
{
    Page page = buffer->pageinfo.page;
    GinInitPage(page, GIN_DELETED, buffer->pageinfo.pagesize);
    PageSetLSN(page, buffer->lsn);
}

void GinRedoCreateIndexBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == GIN_CREATE_INDEX_META_BLOCK_NUM) {
        ginRedoCreateIndexOperatorMetaPage(bufferinfo);
    } else {
        ginRedoCreateIndexOperatorRootPage(bufferinfo);
    }
}

void GinRedoCreatePTreeBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == GIN_CREATE_P_TREE_BLOCK_NUM) {
        char* maindata = XLogBlockDataGetMainData(datadecode, NULL);
        ginRedoCreatePTreeOperatorPage(bufferinfo, maindata);
    }
}

void GinRedoInsertBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        if (XLogBlockDataGetBlockId(datadecode) == GIN_INSERT_BLOCK_NUM) {
            ginxlogInsert* maindata = (ginxlogInsert*)XLogBlockDataGetMainData(datadecode, NULL);
            bool isLeaf = (maindata->flags & GIN_INSERT_ISLEAF) != 0;
            Size blkdatalen;
            char* blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);

            BlockNumber rightChildBlkno = InvalidBlockNumber;
            if (!isLeaf) {
                char* payload = ((char*)maindata) + sizeof(ginxlogInsert);
                payload += sizeof(BlockIdData);  // leftChildBlkno
                rightChildBlkno = BlockIdGetBlockNumber((BlockId)payload);
            }

            if (maindata->flags & GIN_INSERT_ISDATA) {
                ginRedoInsertData(bufferinfo, isLeaf, rightChildBlkno, blkdata);
            } else {
                ginRedoInsertEntry(bufferinfo, isLeaf, rightChildBlkno, blkdata);
            }
        } else {
            ginRedoClearIncompleteSplitOperatorPage(bufferinfo);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GinRedoSplitBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == GIN_SPLIT_CLEAN_IMCOMPELTE_BLOCK_NUM) {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            ginRedoClearIncompleteSplitOperatorPage(bufferinfo);
            MakeRedoBufferDirty(bufferinfo);
        }
    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action != BLK_RESTORED) {
            ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("GinRedoSplitBlock did not contain a full-page image of %u page",
                        XLogBlockDataGetBlockId(datadecode))));
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GinRedoVacuumPageBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action != BLK_RESTORED) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("GinRedoVacuumPageBlock did not contain a full-page image of %u page",
                    XLogBlockDataGetBlockId(datadecode))));
    }
    MakeRedoBufferDirty(bufferinfo);
}

void GinRedoVacuumDataLeafPageBlock(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char* blkdata = XLogBlockDataGetBlockData(datadecode, NULL);
        ginRedoVacuumDataOperatorLeafPage(bufferinfo, blkdata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GinRedoDeletePageBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;

    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        if (XLogBlockDataGetBlockId(datadecode) == GIN_DELETE_D_PAGE_BLOCK_NUM) {
            ginRedoDeletePageOperatorCurPage(bufferinfo);
        } else if (XLogBlockDataGetBlockId(datadecode) == GIN_DELETE_P_PAGE_BLOCK_NUM) {
            char* data = XLogBlockDataGetMainData(datadecode, NULL);
            ginRedoDeletePageOperatorParentPage(bufferinfo, data);
        } else {
            char* data = XLogBlockDataGetMainData(datadecode, NULL);
            ginRedoDeletePageOperatorLeftPage(bufferinfo, data);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GinRedoUpdateMetapageBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;

    if (XLogBlockDataGetBlockId(datadecode) == GIN_META_PAGE_BLOCK_NUM) {
        ginxlogUpdateMeta* data = (ginxlogUpdateMeta*)XLogBlockDataGetMainData(datadecode, NULL);
        ginRedoUpdateOperatorMetapage(bufferinfo, (void*)data);
        MakeRedoBufferDirty(bufferinfo);
    } else {
        XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action == BLK_NEEDS_REDO) {
            ginxlogUpdateMeta* data = (ginxlogUpdateMeta*)XLogBlockDataGetMainData(datadecode, NULL);
            if (data->ntuples > 0) {
                Size totaltupsize;
                void* payload = (void*)XLogBlockDataGetBlockData(datadecode, &totaltupsize);

                ginRedoUpdateOperatorTailPage(bufferinfo, payload, totaltupsize, data->ntuples);
                MakeRedoBufferDirty(bufferinfo);
            } else if (data->prevTail != InvalidBlockNumber) {
                ginRedoUpdateAddNewTail(bufferinfo, data->newRightlink);
                MakeRedoBufferDirty(bufferinfo);
            }
        }
    }
}

void GinRedoInsertListPageBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        void* data = (void*)XLogBlockDataGetMainData(datadecode, NULL);
        Size totaltupsize;
        void* payload = (void*)XLogBlockDataGetBlockData(datadecode, &totaltupsize);
        ginRedoInsertListPageOperatorPage(bufferinfo, data, payload, totaltupsize);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void GinRedoDeleteListPagesBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    XLogBlockDataParse* datadecode = blockdatarec;
    if (XLogBlockDataGetBlockId(datadecode) == GIN_DELETE_LIST_META_PAGE_BLOCK_NUM) {
        ginxlogDeleteListPages* data = (ginxlogDeleteListPages*)XLogBlockDataGetMainData(datadecode, NULL);
        ginRedoDeleteListPagesOperatorPage(bufferinfo, (void*)&(data->metadata));
    } else {
        ginRedoDeleteListPagesMarkDelete(bufferinfo);
    }
    MakeRedoBufferDirty(bufferinfo);
}

void GinRedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_GIN_CREATE_INDEX:
            GinRedoCreateIndexBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_CREATE_PTREE:
            GinRedoCreatePTreeBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_INSERT:
            GinRedoInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_SPLIT:
            GinRedoSplitBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_VACUUM_PAGE:
            GinRedoVacuumPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            GinRedoVacuumDataLeafPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_DELETE_PAGE:
            GinRedoDeletePageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_UPDATE_META_PAGE:
            GinRedoUpdateMetapageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_INSERT_LISTPAGE:
            GinRedoInsertListPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_GIN_DELETE_LISTPAGE:
            GinRedoDeleteListPagesBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("GinRedoDataBlock: unknown op code %hhu", info)));
            break;
    }
}
