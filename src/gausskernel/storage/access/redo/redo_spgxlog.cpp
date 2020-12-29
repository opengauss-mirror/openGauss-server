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
 * spgxlog.cpp
 *    parse spgist xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/spgxlog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/spgist_private.h"
#include "utils/rel_gs.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"

#include "storage/standby.h"
#include "utils/memutils.h"
#include "access/multi_redo_api.h"

typedef enum {
    SPG_CREATE_INDEX_META_BLOCK_NUM = 0,
    SPG_CREATE_INDEX_ROOT_BLOCK_NUM,
    SPG_CREATE_INDEX_LEAF_BLOCK_NUM,
} XLogSpgCreateIndexBlockEnum;

typedef enum {
    SPG_ADD_LEAF_LEAF_BLOCK_NUM = 0,
    SPG_ADD_LEAF_PARENT_BLOCK_NUM,
} XLogSpgAddLeafBlockEnum;

typedef enum {
    SPG_MOVE_LEAF_DST_BLOCK_NUM = 0,
    SPG_MOVE_LEAF_SRC_BLOCK_NUM,
    SPG_MOVE_LEAF_PARENT_BLOCK_NUM,
} XLogSpgMoveLeafBlockEnum;

typedef enum {
    SPG_ADD_NODE_OLD_BLOCK_NUM = 0,
    SPG_ADD_NODE_NEW_BLOCK_NUM,
    SPG_ADD_NODE_PARENT_BLOCK_NUM,
} XLogSpgAddNodeBlockEnum;

typedef enum {
    SPG_SPLIT_TUPLE_OLD_BLOCK_NUM = 0,
    SPG_SPLIT_TUPLE_NEW_BLOCK_NUM,
} XLogSpgSplitTupleBlockEnum;

typedef enum {
    SPG_PICK_SPLIT_SRC_BLOCK_NUM = 0,
    SPG_PICK_SPLIT_DST_BLOCK_NUM,
    SPG_PICK_SPLIT_INNER_BLOCK_NUM,
    SPG_PICK_SPLIT_PARENT_BLOCK_NUM,
} XLogSpgPickSplitBlockEnum;

typedef enum {
    SPG_PICK_VACUUM_LEAF_BLOCK_NUM = 0,
} XLogSpgVacuumLeafBlockEnum;

typedef enum {
    SPG_PICK_VACUUM_ROOT_BLOCK_NUM = 0,
} XLogSpgVacuumRootBlockEnum;

typedef enum {
    SPG_PICK_VACUUM_REDIRECT_BLOCK_NUM = 0,
} XLogSpgVacuumRedirectBlockEnum;

void spgRedoCreateIndexOperatorMetaPage(RedoBufferInfo *buffer)
{
    Assert(buffer->blockinfo.blkno == SPGIST_METAPAGE_BLKNO);
    Page page = buffer->pageinfo.page;
    SpGistInitMetapage(page);
    PageSetLSN(page, buffer->lsn);
}

void spgRedoCreateIndexOperatorRootPage(RedoBufferInfo *buffer)
{
    Assert(buffer->blockinfo.blkno == SPGIST_ROOT_BLKNO);
    Page page = buffer->pageinfo.page;
    Assert(buffer->pageinfo.pagesize == BLCKSZ);
    SpGistInitPage(page, SPGIST_LEAF);
    PageSetLSN(page, buffer->lsn);
}

void spgRedoCreateIndexOperatorLeafPage(RedoBufferInfo *buffer)
{
    Assert(buffer->blockinfo.blkno == SPGIST_NULL_BLKNO);
    Page page = buffer->pageinfo.page;
    Assert(buffer->pageinfo.pagesize == BLCKSZ);
    SpGistInitPage(page, SPGIST_LEAF | SPGIST_NULLS);
    PageSetLSN(page, buffer->lsn);
}

void spgRedoAddLeafOperatorPage(RedoBufferInfo *bufferinfo, void *recorddata)
{
    Page page = bufferinfo->pageinfo.page;
    char *ptr = (char *)recorddata;
    spgxlogAddLeaf *xldata = (spgxlogAddLeaf *)ptr;
    SpGistLeafTuple leafTuple;

    /* we assume this is adequately aligned */
    ptr += sizeof(spgxlogAddLeaf);
    leafTuple = (SpGistLeafTuple)ptr;

    if (xldata->newPage) {
        Assert(bufferinfo->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(page, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
    }

    /* insert new tuple */
    if (xldata->offnumLeaf != xldata->offnumHeadLeaf) {
        /* normal cases, tuple was added by SpGistPageAddNewItem */
        addOrReplaceTuple(page, (Item)leafTuple, leafTuple->size, xldata->offnumLeaf);

        /* update head tuple's chain link if needed */
        if (xldata->offnumHeadLeaf != InvalidOffsetNumber) {
            SpGistLeafTuple head;

            head = (SpGistLeafTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumHeadLeaf));
            Assert(head->nextOffset == leafTuple->nextOffset);
            head->nextOffset = xldata->offnumLeaf;
        }
    } else {
        /* replacing a DEAD tuple */
        PageIndexTupleDelete(page, xldata->offnumLeaf);
        if (PageAddItem(page, (Item)leafTuple, leafTuple->size, xldata->offnumLeaf, false, false) != xldata->offnumLeaf)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("failed to add item of size %u to SPGiST index page", leafTuple->size)));
    }

    PageSetLSN(page, bufferinfo->lsn);
}

void spgRedoAddLeafOperatorParent(RedoBufferInfo *bufferinfo, void *recorddata, BlockNumber blknoLeaf)
{
    SpGistInnerTuple tuple;
    char *ptr = (char *)recorddata;
    spgxlogAddLeaf *xldata = (spgxlogAddLeaf *)ptr;

    Page page = bufferinfo->pageinfo.page;

    tuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));

    spgUpdateNodeLink(tuple, xldata->nodeI, blknoLeaf, xldata->offnumLeaf);

    PageSetLSN(page, bufferinfo->lsn);
}

void spgRedoMoveLeafsOpratorDstPage(RedoBufferInfo *buffer, void *recorddata, void *insertdata, void *tupledata)
{
    spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *)recorddata;
    OffsetNumber *toInsert = (OffsetNumber *)insertdata;

    int nInsert = xldata->replaceDead ? 1 : xldata->nMoves + 1;

    /* now ptr points to the list of leaf tuples */

    /*
     * In normal operation we would have all three pages (source, dest, and
     * parent) locked simultaneously; but in WAL replay it should be safe to
     * update them one at a time, as long as we do it in the right order.
     */

    /* Insert tuples on the dest page (do first, so redirect is valid) */

    Page page = buffer->pageinfo.page;
    if (xldata->newPage) {
        Assert(buffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(page, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
    }
    char *ptr = (char *)tupledata;
    int i;
    for (i = 0; i < nInsert; i++) {
        SpGistLeafTuple lt = (SpGistLeafTuple)ptr;

        addOrReplaceTuple(page, (Item)lt, lt->size, toInsert[i]);
        ptr += lt->size;
    }

    PageSetLSN(page, buffer->lsn);
}

void spgRedoMoveLeafsOpratorSrcPage(RedoBufferInfo *buffer, void *recorddata, void *insertdata, void *deletedata,
                                    BlockNumber blknoDst, int nInsert)
{
    spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *)recorddata;
    Page page = buffer->pageinfo.page;
    OffsetNumber *toInsert = (OffsetNumber *)insertdata;
    OffsetNumber *toDelete = (OffsetNumber *)deletedata;
    SpGistState state;

    fillFakeState(&state, xldata->stateSrc);

    spgPageIndexMultiDelete(&state, page, toDelete, xldata->nMoves,
                            state.isBuild ? SPGIST_PLACEHOLDER : SPGIST_REDIRECT, SPGIST_PLACEHOLDER, blknoDst,
                            toInsert[nInsert - 1]);

    PageSetLSN(page, buffer->lsn);
}

void spgRedoMoveLeafsOpratorParentPage(RedoBufferInfo *buffer, void *recorddata, void *insertdata, BlockNumber blknoDst,
                                       int nInsert)
{
    spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *)recorddata;
    Page page = buffer->pageinfo.page;
    OffsetNumber *toInsert = (OffsetNumber *)insertdata;
    SpGistInnerTuple tuple;

    tuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));

    spgUpdateNodeLink(tuple, xldata->nodeI, blknoDst, toInsert[nInsert - 1]);

    PageSetLSN(page, buffer->lsn);
}

void spgRedoAddNodeUpdateSrcPage(RedoBufferInfo *buffer, void *recorddata, void *tuple, void *tupleheader)
{
    spgxlogAddNode *xldata = (spgxlogAddNode *)recorddata;
    Page page = buffer->pageinfo.page;
    char *innerTuple = (char *)tuple;
    SpGistInnerTupleData *innerTupleHdr = (SpGistInnerTupleData *)tupleheader;

    PageIndexTupleDelete(page, xldata->offnum);
    if (PageAddItem(page, (Item)innerTuple, innerTupleHdr->size, xldata->offnum, false, false) != xldata->offnum)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add item of size %u to SPGiST index page", innerTupleHdr->size)));

    PageSetLSN(page, buffer->lsn);
}

void spgRedoAddNodeOperatorSrcPage(RedoBufferInfo *buffer, void *recorddata, BlockNumber blknoNew)
{
    spgxlogAddNode *xldata = (spgxlogAddNode *)recorddata;
    Page page = buffer->pageinfo.page;
    SpGistDeadTuple dt;
    SpGistState state;

    fillFakeState(&state, xldata->stateSrc);
    if (state.isBuild)
        dt = spgFormDeadTuple(&state, SPGIST_PLACEHOLDER, InvalidBlockNumber, InvalidOffsetNumber);
    else
        dt = spgFormDeadTuple(&state, SPGIST_REDIRECT, blknoNew, xldata->offnumNew);

    PageIndexTupleDelete(page, xldata->offnum);
    if (PageAddItem(page, (Item)dt, dt->size, xldata->offnum, false, false) != xldata->offnum)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add item of size %u to SPGiST index page", dt->size)));

    if (state.isBuild)
        SpGistPageGetOpaque(page)->nPlaceholder++;
    else
        SpGistPageGetOpaque(page)->nRedirection++;

    /*
     * If parent is in this same page,update it now.
     */
    if (xldata->parentBlk == 0) {
        SpGistInnerTuple parentTuple;

        parentTuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));

        spgUpdateNodeLink(parentTuple, xldata->nodeI, blknoNew, xldata->offnumNew);
    }

    PageSetLSN(page, buffer->lsn);
}

void spgRedoAddNodeOperatorDestPage(RedoBufferInfo *buffer, void *recorddata, void *tuple, void *tupleheader,
                                    BlockNumber blknoNew)
{
    spgxlogAddNode *xldata = (spgxlogAddNode *)recorddata;
    Page page = buffer->pageinfo.page;
    char *innerTuple = (char *)tuple;
    SpGistInnerTupleData *innerTupleHdr = (SpGistInnerTupleData *)tupleheader;

    /*
     * In normal operation we would have all three pages (source, dest,
     * and parent) locked simultaneously; but in WAL replay it should be
     * safe to update them one at a time, as long as we do it in the right
     * order. We must insert the new tuple before replacing the old tuple
     * with the redirect tuple.
     */

    /* Install new tuple first so redirect is valid */
    if (xldata->newPage) {
        Assert(buffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(page, 0);
    }

    addOrReplaceTuple(page, (Item)innerTuple, innerTupleHdr->size, xldata->offnumNew);

    /*
     * If parent is in this same page, update it now.
     */
    if (xldata->parentBlk == 1) {
        SpGistInnerTuple parentTuple;

        parentTuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));

        spgUpdateNodeLink(parentTuple, xldata->nodeI, blknoNew, xldata->offnumNew);
    }
    PageSetLSN(page, buffer->lsn);
}

void spgRedoAddNodeOperatorParentPage(RedoBufferInfo *buffer, void *recorddata, BlockNumber blknoNew)
{
    spgxlogAddNode *xldata = (spgxlogAddNode *)recorddata;
    Page page = buffer->pageinfo.page;
    SpGistInnerTuple parentTuple;

    parentTuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));

    spgUpdateNodeLink(parentTuple, xldata->nodeI, blknoNew, xldata->offnumNew);

    PageSetLSN(page, buffer->lsn);
}

void spgRedoSplitTupleOperatorDestPage(RedoBufferInfo *buffer, void *recorddata, void *tuple)
{
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *)recorddata;
    Page page = buffer->pageinfo.page;
    SpGistInnerTuple postfixTuple = (SpGistInnerTuple)tuple;

    if (xldata->newPage) {
        /* SplitTuple is not used for nulls pages */
        Assert(buffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(page, 0);
    }

    page = BufferGetPage(buffer->buf);

    addOrReplaceTuple(page, (Item)postfixTuple, postfixTuple->size, xldata->offnumPostfix);

    PageSetLSN(page, buffer->lsn);
}

void spgRedoSplitTupleOperatorSrcPage(RedoBufferInfo *buffer, void *recorddata, void *pretuple, void *posttuple)
{
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *)recorddata;
    Page page = buffer->pageinfo.page;
    SpGistInnerTuple prefixTuple = (SpGistInnerTuple)pretuple;
    SpGistInnerTuple postfixTuple = (SpGistInnerTuple)posttuple;

    page = BufferGetPage(buffer->buf);

    PageIndexTupleDelete(page, xldata->offnumPrefix);
    if (PageAddItem(page, (Item)prefixTuple, prefixTuple->size, xldata->offnumPrefix, false, false) !=
        xldata->offnumPrefix)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add item of size %u to SPGiST index page", prefixTuple->size)));

    if (xldata->postfixBlkSame)
        addOrReplaceTuple(page, (Item)postfixTuple, postfixTuple->size, xldata->offnumPostfix);

    PageSetLSN(page, buffer->lsn);
}

/* restore leaf tuples to src and/or dest page */
void spgRedoPickSplitRestoreLeafTuples(RedoBufferInfo *buffer, void *recorddata, bool destflag, void *pageselect,
                                       void *insertdata)
{
    char *ptr = (char *)recorddata;
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    Page page = buffer->pageinfo.page;
    uint8 *leafPageSelect = (uint8 *)pageselect;
    OffsetNumber *toInsert = (OffsetNumber *)insertdata;

    int i;
    /* restore leaf tuples to src and/or dest page */
    for (i = 0; i < xldata->nInsert; i++) {
        SpGistLeafTuple lt = (SpGistLeafTuple)ptr;

        ptr += lt->size;

        if ((destflag && leafPageSelect[i]) || ((!destflag) && (!leafPageSelect[i]))) {
            addOrReplaceTuple(page, (Item)lt, lt->size, toInsert[i]);
        }
    }
}

void spgRedoPickSplitOperatorSrcPage(RedoBufferInfo *srcBuffer, void *recorddata, void *deleteoffset,
                                     BlockNumber blknoInner, void *pageselect, void *insertdata)
{
    char *ptr = (char *)recorddata;
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    Page srcPage = srcBuffer->pageinfo.page;
    OffsetNumber *toDelete = (OffsetNumber *)deleteoffset;
    SpGistState state;

    if (xldata->initSrc) {
        Assert(srcBuffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(srcPage, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
    } else {
        fillFakeState(&state, xldata->stateSrc);
        /*
         * We have it a bit easier here than in doPickSplit(),
         * because we know the inner tuple's location already, so
         * we can inject the correct redirection tuple now.
         */
        if (!state.isBuild)
            spgPageIndexMultiDelete(&state, srcPage, toDelete, xldata->nDelete, SPGIST_REDIRECT, SPGIST_PLACEHOLDER,
                                    blknoInner, xldata->offnumInner);
        else
            spgPageIndexMultiDelete(&state, srcPage, toDelete, xldata->nDelete, SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                                    InvalidBlockNumber, InvalidOffsetNumber);
    }

    spgRedoPickSplitRestoreLeafTuples(srcBuffer, recorddata, false, pageselect, insertdata);

    /* don't update LSN etc till we're done with it */
    /* modify for batchlsn, don't markdirty */
    PageSetLSN(srcPage, srcBuffer->lsn);
}

void spgRedoPickSplitOperatorDestPage(RedoBufferInfo *destBuffer, void *recorddata, void *pageselect, void *insertdata)
{
    char *ptr = (char *)recorddata;
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    Page destPage = destBuffer->pageinfo.page;

    if (xldata->initDest) {
        Assert(destBuffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(destPage, SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
    }

    spgRedoPickSplitRestoreLeafTuples(destBuffer, recorddata, true, pageselect, insertdata);
    PageSetLSN(destPage, destBuffer->lsn);
}

void spgRedoPickSplitOperatorInnerPage(RedoBufferInfo *innerBuffer, void *recorddata, void *tuple, void *tupleheader,
                                       BlockNumber blknoInner)
{
    char *ptr = (char *)recorddata;
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    Page page = innerBuffer->pageinfo.page;
    char *innerTuple = (char *)tuple;
    SpGistInnerTupleData *innerTupleHdr = (SpGistInnerTupleData *)tupleheader;

    if (xldata->initInner) {
        Assert(innerBuffer->pageinfo.pagesize == BLCKSZ);
        SpGistInitPage(page, (xldata->storesNulls ? SPGIST_NULLS : 0));
    }

    addOrReplaceTuple(page, (Item)innerTuple, innerTupleHdr->size, xldata->offnumInner);

    /* if inner is also parent, update link while we're here */
    if (xldata->innerIsParent) {
        SpGistInnerTuple parent;

        parent = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));
        spgUpdateNodeLink(parent, xldata->nodeI, blknoInner, xldata->offnumInner);
    }

    PageSetLSN(page, innerBuffer->lsn);
}

void spgRedoPickSplitOperatorParentPage(RedoBufferInfo *parentBuffer, void *recorddata, BlockNumber blknoInner)
{
    char *ptr = (char *)recorddata;
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    SpGistInnerTuple parent;

    Page page = parentBuffer->pageinfo.page;
    parent = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, xldata->offnumParent));
    spgUpdateNodeLink(parent, xldata->nodeI, blknoInner, xldata->offnumInner);

    PageSetLSN(page, parentBuffer->lsn);
}

void spgRedoVacuumLeafOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    char *ptr = (char *)recorddata;
    spgxlogVacuumLeaf *xldata = (spgxlogVacuumLeaf *)ptr;
    Page page = buffer->pageinfo.page;

    OffsetNumber *toDead = NULL;
    OffsetNumber *toPlaceholder = NULL;
    OffsetNumber *moveSrc = NULL;
    OffsetNumber *moveDest = NULL;
    OffsetNumber *chainSrc = NULL;
    OffsetNumber *chainDest = NULL;
    SpGistState state;
    int i;

    fillFakeState(&state, xldata->stateSrc);

    ptr += sizeof(spgxlogVacuumLeaf);
    toDead = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nDead;
    toPlaceholder = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nPlaceholder;
    moveSrc = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMove;
    moveDest = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMove;
    chainSrc = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nChain;
    chainDest = (OffsetNumber *)ptr;

    spgPageIndexMultiDelete(&state, page, toDead, xldata->nDead, SPGIST_DEAD, SPGIST_DEAD, InvalidBlockNumber,
                            InvalidOffsetNumber);

    spgPageIndexMultiDelete(&state, page, toPlaceholder, xldata->nPlaceholder, SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                            InvalidBlockNumber, InvalidOffsetNumber);

    /* see comments in vacuumLeafPage() */
    for (i = 0; i < xldata->nMove; i++) {
        ItemId idSrc = PageGetItemId(page, moveSrc[i]);
        ItemId idDest = PageGetItemId(page, moveDest[i]);
        ItemIdData tmp;

        tmp = *idSrc;
        *idSrc = *idDest;
        *idDest = tmp;
    }

    spgPageIndexMultiDelete(&state, page, moveSrc, xldata->nMove, SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                            InvalidBlockNumber, InvalidOffsetNumber);

    for (i = 0; i < xldata->nChain; i++) {
        SpGistLeafTuple lt;

        lt = (SpGistLeafTuple)PageGetItem(page, PageGetItemId(page, chainSrc[i]));
        Assert(lt->tupstate == SPGIST_LIVE);
        lt->nextOffset = chainDest[i];
    }

    PageSetLSN(page, buffer->lsn);
}

void spgRedoVacuumRootOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    char *ptr = (char *)recorddata;
    spgxlogVacuumRoot *xldata = (spgxlogVacuumRoot *)ptr;
    Page page = buffer->pageinfo.page;
    OffsetNumber *toDelete = NULL;

    ptr += sizeof(spgxlogVacuumRoot);
    toDelete = (OffsetNumber *)ptr;

    /* The tuple numbers are in order */
    PageIndexMultiDelete(page, toDelete, xldata->nDelete);

    PageSetLSN(page, buffer->lsn);
}

void spgRedoVacuumRedirectOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    char *ptr = (char *)recorddata;
    spgxlogVacuumRedirect *xldata = (spgxlogVacuumRedirect *)ptr;
    Page page = buffer->pageinfo.page;
    OffsetNumber *itemToPlaceholder = NULL;
    int i;

    itemToPlaceholder = xldata->offsets;
    /* Convert redirect pointers to plain placeholders */
    for (i = 0; i < xldata->nToPlaceholder; i++) {
        SpGistDeadTuple dt;

        dt = (SpGistDeadTuple)PageGetItem(page, PageGetItemId(page, itemToPlaceholder[i]));
        Assert(dt->tupstate == SPGIST_REDIRECT);
        dt->tupstate = SPGIST_PLACEHOLDER;
        ItemPointerSetInvalid(&dt->pointer);
    }

    SpGistPageOpaque opaque = SpGistPageGetOpaque(page);
    Assert(opaque->nRedirection >= xldata->nToPlaceholder);
    opaque->nRedirection -= xldata->nToPlaceholder;
    opaque->nPlaceholder += xldata->nToPlaceholder;

    /* Remove placeholder tuples at end of page */
    if (xldata->firstPlaceholder != InvalidOffsetNumber) {
        int max = PageGetMaxOffsetNumber(page);
        OffsetNumber *toDelete = NULL;

        toDelete = (OffsetNumber *)palloc(sizeof(OffsetNumber) * max);

        for (i = xldata->firstPlaceholder; i <= max; i++)
            toDelete[i - xldata->firstPlaceholder] = i;

        i = max - xldata->firstPlaceholder + 1;
        Assert(opaque->nPlaceholder >= i);
        opaque->nPlaceholder -= i;

        /* The array is sorted, so can use PageIndexMultiDelete */
        PageIndexMultiDelete(page, toDelete, i);

        pfree(toDelete);
    }

    PageSetLSN(page, buffer->lsn);
}

static XLogRecParseState *SpgXlogCreateIndexParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_CREATE_INDEX_META_BLOCK_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_CREATE_INDEX_ROOT_BLOCK_NUM, blockstate);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_CREATE_INDEX_LEAF_BLOCK_NUM, blockstate);
    *blocknum = 3;
    return recordstatehead;
}

static XLogRecParseState *SpgXlogAddLeafParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_CREATE_INDEX_META_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    char *ptr = XLogRecGetData(record);
    spgxlogAddLeaf *xldata = (spgxlogAddLeaf *)ptr;

    if (xldata->offnumParent != InvalidOffsetNumber) {
        XLogRecParseState *blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, SPG_ADD_LEAF_PARENT_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    return recordstatehead;
}

static XLogRecParseState *SpgXlogMoveLeafsParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_MOVE_LEAF_DST_BLOCK_NUM, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_MOVE_LEAF_SRC_BLOCK_NUM, blockstate);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_MOVE_LEAF_PARENT_BLOCK_NUM, blockstate);

    *blocknum = 3;
    return recordstatehead;
}

static XLogRecParseState *SpgXlogAddNodeParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_ADD_NODE_OLD_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecParseState *blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, SPG_ADD_NODE_NEW_BLOCK_NUM, blockstate);
        ++(*blocknum);

        char *ptr = XLogRecGetData(record);
        spgxlogAddNode *xldata = (spgxlogAddNode *)ptr;
        if (xldata->parentBlk == 2) {
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            XLogRecSetBlockDataState(record, SPG_ADD_NODE_PARENT_BLOCK_NUM, blockstate);
            ++(*blocknum);
        }
    }

    return recordstatehead;
}

static XLogRecParseState *SpgXlogSplitTupleParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_SPLIT_TUPLE_OLD_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    char *ptr = XLogRecGetData(record);
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *)ptr;

    if (!xldata->postfixBlkSame) {
        XLogRecParseState *blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, SPG_SPLIT_TUPLE_NEW_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    return recordstatehead;
}

static XLogRecParseState *SpgXlogPickSplitParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_PICK_SPLIT_SRC_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecParseState *blockstate = NULL;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        XLogRecSetBlockDataState(record, SPG_PICK_SPLIT_DST_BLOCK_NUM, blockstate);
        ++(*blocknum);
    }

    XLogRecParseState *blockstate = NULL;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_PICK_SPLIT_INNER_BLOCK_NUM, blockstate);
    ++(*blocknum);

    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
    XLogRecSetBlockDataState(record, SPG_PICK_SPLIT_PARENT_BLOCK_NUM, blockstate);
    ++(*blocknum);

    return recordstatehead;
}

static XLogRecParseState *SpgXlogVacuumLeafParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_PICK_VACUUM_LEAF_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    return recordstatehead;
}

static XLogRecParseState *SpgXlogVacuumRootParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_PICK_VACUUM_ROOT_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    return recordstatehead;
}

static XLogRecParseState *SpgXlogVacuumRedirectParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    XLogRecSetBlockDataState(record, SPG_PICK_VACUUM_REDIRECT_BLOCK_NUM, recordstatehead);
    *blocknum = 1;

    return recordstatehead;
}

XLogRecParseState *SpgRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    *blocknum = 0;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    switch (info) {
        case XLOG_SPGIST_CREATE_INDEX:
            recordblockstate = SpgXlogCreateIndexParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_ADD_LEAF:
            recordblockstate = SpgXlogAddLeafParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_MOVE_LEAFS:
            recordblockstate = SpgXlogMoveLeafsParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_ADD_NODE:
            recordblockstate = SpgXlogAddNodeParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_SPLIT_TUPLE:
            recordblockstate = SpgXlogSplitTupleParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_PICKSPLIT:
            recordblockstate = SpgXlogPickSplitParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_VACUUM_LEAF:
            recordblockstate = SpgXlogVacuumLeafParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_VACUUM_ROOT:
            recordblockstate = SpgXlogVacuumRootParseBlock(record, blocknum);
            break;
        case XLOG_SPGIST_VACUUM_REDIRECT:
            recordblockstate = SpgXlogVacuumRedirectParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("spg_redo: unknown op code %u", info)));
    }

    return NULL;
}
