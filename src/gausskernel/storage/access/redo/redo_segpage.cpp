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
 * redo_segpage.cpp
 *    parse segpage xlog
 *
 * IDENTIFICATION
 *
 * src/gausskernel/storage/access/redo/redo_segpage.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogproc.h"
#include "access/redo_common.h"
#include "access/double_write.h"
#include "commands/tablespace.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr/fd.h"

static XLogRecParseState *segpage_redo_parse_seg_truncate_to_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    BlockNumber nblocks = *(BlockNumber *)XLogRecGetBlockData(record, 0, NULL);
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blknum;
    XLogRecGetBlockTag(record, 0, &rnode, &forknum, &blknum);
    rnode.relNode = blknum;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, forknum, nblocks);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_DDL_TYPE, filenode, recordstatehead);

    XLogRecSetBlockDdlState(&(recordstatehead->blockparse.extra_rec.blocksegddlrec.blockddlrec),
                            BLOCK_DDL_TRUNCATE_RELNODE, NULL);

    XLogRecSetBlockDataStateContent(record, 0, &(recordstatehead->blockparse.extra_rec.blocksegddlrec.blockdatarec));

    return recordstatehead;
}

static XLogRecParseState *segpage_redo_parse_space_drop(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    char *data = (char *)XLogRecGetData(record);
    RelFileNode rnode;
    rnode.spcNode = *(Oid *)data;
    rnode.dbNode = *(Oid *)(data + sizeof(Oid));
    rnode.relNode = InvalidOid;
    rnode.opt = 0;
    rnode.bucketNode = InvalidBktId;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(&rnode, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_SEG_SPACE_DROP, filenode, recordstatehead);

    return recordstatehead;
}

static XLogRecParseState *segpage_redo_parse_space_shrink(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    XLogDataSpaceShrink *xlog_data = (XLogDataSpaceShrink *)XLogRecGetData(record);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    /* As block number in file node is useless for space shrink, we use it to store target size */
    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(&xlog_data->rnode, InvalidBackendId, xlog_data->forknum, xlog_data->target_size);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_SEG_SPACE_SHRINK, filenode, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *segpage_parse_segment_extend_page(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, 0, recordstatehead);

    XLogRecParseState *blockstate = NULL;
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);

    XLogRecSetBlockDataState(record, 1, blockstate);

    return recordstatehead;
}

static XLogRecParseState *segpage_parse_atomic_xlog_page(XLogReaderState *record, uint32 *blocknum)
{
    int nbuffers = *(int *)XLogRecGetData(record);
    Assert((nbuffers-1) == record->max_block_id);
    XLogRecParseState *recordstatehead = NULL;

    for (int i = 0; i < nbuffers; i++) {
        XLogRecParseState *blockstate = NULL;
        if (recordstatehead == NULL) {
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, NULL);
            recordstatehead = blockstate;
        } else {
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        }

        XLogRecSetBlockDataState(record, i, blockstate);
    }
    return recordstatehead;
}

typedef XLogRecParseState *(child_xlog_page_parse_func)(XLogReaderState *record, uint32 *blocknum);
static XLogRecParseState *segpage_redo_parse_child_xlog(XLogReaderState *record, uint32 *blocknum,
    child_xlog_page_parse_func parse_fun, XLogBlockParseEnum parsetype)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecParseState *childState = parse_fun(record, blocknum);
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, parsetype, filenode, recordstatehead);
    XLogRecSetSegFullSyncState(&recordstatehead->blockparse.extra_rec.blocksegfullsyncrec, childState);

    return recordstatehead;
}

static XLogRecParseState *segpage_redo_parse_extent_group(XLogReaderState *record, uint32 *blocknum)
{
    char *data = XLogRecGetData(record);
    RelFileNode *rnode = (RelFileNode *)data;
    ForkNumber forknum = *(ForkNumber *)(data + sizeof(RelFileNode));
    
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(rnode, InvalidBackendId, forknum, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_SEG_FULL_SYNC_TYPE, filenode, recordstatehead);

    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

static XLogRecParseState *segpage_parse_init_map_page(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, 0, recordstatehead);

    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

static XLogRecParseState *segpage_parse_init_inverse_page(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, 0, recordstatehead);

    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

static XLogRecParseState *segpage_parse_add_new_group(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    XLogRecSetBlockDataState(record, 0, recordstatehead);

    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

static XLogRecParseState *segpage_parse_new_page(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(NULL, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_SEG_FULL_SYNC_TYPE, filenode, recordstatehead);
    XLogRecSetSegNewPageInfo(&recordstatehead->blockparse.extra_rec.blocksegnewpageinfo, XLogRecGetData(record),
        XLogRecGetDataLen(record));
    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

XLogRecParseState *segpage_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_SEG_TRUNCATE:
            recordstatehead = segpage_redo_parse_seg_truncate_to_block(record, blocknum);
            break;
        case XLOG_SEG_SPACE_DROP:
            recordstatehead = segpage_redo_parse_space_drop(record, blocknum);
            break;
        case XLOG_SEG_SPACE_SHRINK:
            recordstatehead = segpage_redo_parse_space_shrink(record, blocknum);
            break;
        case XLOG_SEG_SEGMENT_EXTEND:
            recordstatehead = segpage_redo_parse_child_xlog(record, blocknum, segpage_parse_segment_extend_page,
                BLOCK_DATA_SEG_EXTEND);
            break;
        case XLOG_SEG_ATOMIC_OPERATION:
            recordstatehead = segpage_redo_parse_child_xlog(record, blocknum, segpage_parse_atomic_xlog_page,
                BLOCK_DATA_SEG_FULL_SYNC_TYPE);
            break;
        case XLOG_SEG_CREATE_EXTENT_GROUP:
            recordstatehead = segpage_redo_parse_extent_group(record, blocknum);
            break;
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
            recordstatehead = segpage_redo_parse_child_xlog(record, blocknum, segpage_parse_init_inverse_page,
                BLOCK_DATA_SEG_FULL_SYNC_TYPE);
            break;
        case XLOG_SEG_INIT_MAPPAGE:
            recordstatehead = segpage_redo_parse_child_xlog(record, blocknum, segpage_parse_init_map_page,
                BLOCK_DATA_SEG_FULL_SYNC_TYPE);
            break;
        case XLOG_SEG_ADD_NEW_GROUP:
            recordstatehead = segpage_redo_parse_child_xlog(record, blocknum, segpage_parse_add_new_group,
                BLOCK_DATA_SEG_FULL_SYNC_TYPE);
            break;
        case XLOG_SEG_NEW_PAGE:
            recordstatehead = segpage_parse_new_page(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("segpage_redo_parse_to_block: unknown op code %u", info)));
            break;
    }
    return recordstatehead;
}

void ProcRemainAtomicOperation(Buffer buf, DecodedXLogBlockOp *decoded_op, TransactionId xid)
{
    for (int j = 0; j < decoded_op->operations; j++) {
        redo_xlog_deal_alloc_seg(decoded_op->op[j], buf, decoded_op->data[j], decoded_op->data_len[j], xid);
        if (decoded_op->op[j] == SPCXLOG_SHRINK_SEGHEAD_UPDATE) {
            XLogMoveExtent *move_extent_xlog = (XLogMoveExtent *)(decoded_op->data[j]);
            move_extent_flush_buffer(move_extent_xlog);
        }
    }
}

static void SegPageRedoAtomicOperationBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogRedoAction action = XLogCheckBlockDataRedoAction(blockdatarec, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = XLogBlockDataGetBlockData(blockdatarec, &blkdatalen);
        SegmentCheck(blkdatalen != 0);
        DecodedXLogBlockOp decoded_op = XLogAtomicDecodeBlockData(blkdata, blkdatalen);

        for (int j = 0; j < decoded_op.operations; j++) {
            redo_atomic_xlog_dispatch(decoded_op.op[j], bufferinfo, decoded_op.data[j]);
        }

        PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
        MakeRedoBufferDirty(bufferinfo);

        bool is_need_log_remain_segs = IsNeedLogRemainSegs(blockhead->end_ptr);
        if (is_need_log_remain_segs) {
            ProcRemainAtomicOperation(bufferinfo->buf, &decoded_op, blockhead->xl_xid);
        }
    }
}

static void SegPageRedoSegmentExtend(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogRedoAction action = XLogCheckBlockDataRedoAction(blockdatarec, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        if (XLogBlockDataGetBlockId(blockdatarec) == 0) {
            Size blkdatalen;
            char *blkdata = XLogBlockDataGetBlockData(blockdatarec, &blkdatalen);
            XLogDataSegmentExtend *xlog_data = (XLogDataSegmentExtend *)blkdata;
            SegmentHead *seghead = (SegmentHead *)PageGetContents(bufferinfo->pageinfo.page);
            if (seghead->nblocks != xlog_data->old_nblocks) {
                ereport(PANIC, (errmsg("extreme rto:data inconsistent when redo seghead_extend, nblocks is %u on disk,"
                                       " but should be %u according to xlog",
                                       seghead->nblocks, xlog_data->old_nblocks)));
            }
            seghead->nblocks = xlog_data->new_nblocks;
            PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
        } else if (XLogBlockDataGetBlockId(blockdatarec) == 1) {
            memset_s(bufferinfo->pageinfo.page, BLCKSZ, 0, BLCKSZ);
            PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
        } else {
            ereport(PANIC, (errmsg("SegPageRedoSegmentExtend block id error")));
        }

        MakeRedoBufferDirty(bufferinfo);
    }
}


static void SegPageRedoInitMapPage(XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    BlockNumber first_page = *(BlockNumber *)XLogBlockDataGetMainData(blockdatarec, NULL);
    eg_init_bitmap_page_content(bufferinfo->pageinfo.page, first_page);
    PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
    MakeRedoBufferDirty(bufferinfo);
}

static void SegPageRedoInitInversePointPage(RedoBufferInfo *bufferinfo)
{
    SegPageInit(bufferinfo->pageinfo.page, BLCKSZ);
    PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
    MakeRedoBufferDirty(bufferinfo);
}

static void SegPageRedoAddNewGroup(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogRedoAction action = XLogCheckBlockDataRedoAction(blockdatarec, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        xl_new_map_group_info_t *new_group = (xl_new_map_group_info_t *)XLogBlockDataGetMainData(blockdatarec, NULL);
        df_map_head_t *map_head = (df_map_head_t *)PageGetContents(bufferinfo->pageinfo.page);
        map_head->group_count = new_group->group_count;
        SegmentCheck(map_head->group_count > 0);

        df_map_group_t *map_group = &map_head->groups[map_head->group_count - 1];
        map_group->first_map = new_group->first_map_pageno;
        map_group->page_count = new_group->group_size;
        map_group->free_page = 0;
        PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);

        MakeRedoBufferDirty(bufferinfo);
    }
}


void SegPageRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_SEG_ATOMIC_OPERATION:
            SegPageRedoAtomicOperationBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_SEG_SEGMENT_EXTEND:
            SegPageRedoSegmentExtend(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_SEG_INIT_MAPPAGE:
            SegPageRedoInitMapPage(blockdatarec, bufferinfo);
            break;
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
            SegPageRedoInitInversePointPage(bufferinfo);
            break;
        case XLOG_SEG_ADD_NEW_GROUP:
            SegPageRedoAddNewGroup(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("SegPageRedoDataBlock block id error")));
            break;
    }
}

void SegPageRedoExtendGroup(XLogBlockHead *blockHead)
{
    /* Create tablespace directory on the standby */
    TablespaceCreateDbspace(blockHead->spcNode, blockHead->dbNode, true);
    /* Create SegSpace object in memory */
    SegSpace *spc = spc_init_space_node(blockHead->spcNode, blockHead->dbNode);
    eg_init_data_files(&spc->extent_group[EXTENT_TYPE_TO_GROUPID(blockHead->relNode)][blockHead->forknum], true,
        blockHead->end_ptr);
}

void SegPageRedoSpaceShrink(XLogBlockHead *blockhead)
{
    SegSpace *spc = spc_open(blockhead->spcNode, blockhead->dbNode, false);
    if (spc_status(spc) == SpaceDataFileStatus::EMPTY) {
        ereport(LOG, (errmsg("extreme rto redo space shrink, target space <%u, %u, %u> does not exist",
            blockhead->spcNode, blockhead->dbNode, blockhead->relNode)));
        return;
    }
    SegExtentGroup *seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(blockhead->relNode)][blockhead->forknum];
    RelFileNode rnode;
    rnode.spcNode = blockhead->spcNode;
    rnode.dbNode = blockhead->dbNode;
    rnode.relNode = blockhead->relNode;
    rnode.bucketNode = blockhead->bucketNode;
    rnode.opt = 0;
    char *path = relpathperm(rnode, blockhead->forknum);
    ereport(LOG, (errmsg("call space shrink files, filename: %s, xlog lsn: %lX", path, blockhead->end_ptr)));
    pfree(path);
    spc_shrink_files(seg, blockhead->blkno, true);

    /* forget metadata buffer that uses physical block number */
    XLogTruncateRelation(seg->rnode, seg->forknum, blockhead->blkno);
    /* forget data buffer that uses logical block number */
    XLogTruncateSegmentSpace(seg->rnode, seg->forknum, blockhead->blkno);
}

void SegPageRedoSpaceDrop(XLogBlockHead *blockhead)
{
    Assert(blockhead != NULL);
    spc_drop(blockhead->spcNode, blockhead->dbNode, true);
    XLogDropSegmentSpace(blockhead->spcNode, blockhead->dbNode);
}

void SegPageRedoNewPage(XLogBlockHead *blockhead, XLogBlockSegNewPage *newPageInfo)
{
    Assert(newPageInfo->dataLen != 0);
    BufferTag *tag = (BufferTag *)newPageInfo->mainData;

    seg_redo_new_page_copy_and_flush(tag, newPageInfo->mainData + sizeof(BufferTag), blockhead->end_ptr);
}

void MarkSegPageRedoChildPageDirty(RedoBufferInfo *bufferinfo)
{
    BufferDesc *bufDesc = GetBufferDescriptor(bufferinfo->buf - 1);
    if (bufferinfo->dirtyflag || XLByteLT(bufDesc->lsn_on_disk, PageGetLSN(bufferinfo->pageinfo.page))) {
        if (IsSegmentPhysicalRelNode(bufferinfo->blockinfo.rnode)) {
            SegMarkBufferDirty(bufferinfo->buf);
        } else {
            MarkBufferDirty(bufferinfo->buf);
        }
        if (!bufferinfo->dirtyflag && bufferinfo->blockinfo.forknum == MAIN_FORKNUM) {
            int mode = WARNING;
#ifdef USE_ASSERT_CHECKING
            mode = PANIC;
#endif
            const uint32 shiftSz = 32;
            ereport(mode, (errmsg("extreme_rto segment page not mark dirty:lsn %X/%X, lsn_disk %X/%X, \
                                  lsn_page %X/%X, page %u/%u/%u %u",
                                  (uint32)(bufferinfo->lsn >> shiftSz), (uint32)(bufferinfo->lsn),
                                  (uint32)(bufDesc->lsn_on_disk >> shiftSz), (uint32)(bufDesc->lsn_on_disk),
                                  (uint32)(PageGetLSN(bufferinfo->pageinfo.page) >> shiftSz),
                                  (uint32)(PageGetLSN(bufferinfo->pageinfo.page)),
                                  bufferinfo->blockinfo.rnode.spcNode, bufferinfo->blockinfo.rnode.dbNode,
                                  bufferinfo->blockinfo.rnode.relNode, bufferinfo->blockinfo.blkno)));
        }
#ifdef USE_ASSERT_CHECKING
        bufDesc->lsn_dirty = PageGetLSN(bufferinfo->pageinfo.page);
#endif
    }
    if (IsSegmentPhysicalRelNode(bufferinfo->blockinfo.rnode)) {
        SegUnlockReleaseBuffer(bufferinfo->buf); /* release buffer */
    } else {
        UnlockReleaseBuffer(bufferinfo->buf);
    }
}

void SegPageRedoChildState(XLogRecParseState *childStateList)
{
    XLogRecParseState *procState = childStateList;
    RedoTimeCost timeCost1;
    RedoTimeCost timeCost2;

    while (procState != NULL) {
        XLogRecParseState *redoblockstate = procState;
        procState = (XLogRecParseState *)procState->nextrecord;
        RedoBufferInfo bufferinfo = {0};
        (void)XLogBlockRedoForExtremeRTO(redoblockstate, &bufferinfo, false, timeCost1, timeCost2);
        if (bufferinfo.pageinfo.page != NULL) {
            MarkSegPageRedoChildPageDirty(&bufferinfo);
        }
    }

    XLogBlockParseStateRelease(childStateList);
}

void ProcSegPageJustFreeChildState(XLogRecParseState *parseState)
{
    Assert(XLogBlockHeadGetRmid(&parseState->blockparse.blockhead) == RM_SEGPAGE_ID);
    uint8 info = XLogBlockHeadGetInfo(&parseState->blockparse.blockhead) & ~XLR_INFO_MASK;
    if ((info == XLOG_SEG_ATOMIC_OPERATION) || (info == XLOG_SEG_SEGMENT_EXTEND) ||
        (info == XLOG_SEG_INIT_MAPPAGE) || (info == XLOG_SEG_INIT_INVRSPTR_PAGE) ||
        (info == XLOG_SEG_ADD_NEW_GROUP)) {
        XLogRecParseState *child =
            (XLogRecParseState *)parseState->blockparse.extra_rec.blocksegfullsyncrec.childState;
        XLogBlockParseStateRelease(child);
        parseState->blockparse.extra_rec.blocksegfullsyncrec.childState = NULL;
    }
}

void ProcSegPageCommonRedo(XLogRecParseState *parseState)
{
    Assert(XLogBlockHeadGetRmid(&parseState->blockparse.blockhead) == RM_SEGPAGE_ID);
    uint8 info = XLogBlockHeadGetInfo(&parseState->blockparse.blockhead) & ~XLR_INFO_MASK;
    switch (info) {
        // has child list
        case XLOG_SEG_ATOMIC_OPERATION:
        case XLOG_SEG_SEGMENT_EXTEND:
        case XLOG_SEG_INIT_MAPPAGE:
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
        case XLOG_SEG_ADD_NEW_GROUP:
            {
                XLogRecParseState *child =
                    (XLogRecParseState *)parseState->blockparse.extra_rec.blocksegfullsyncrec.childState;
                SegPageRedoChildState(child);
                break;
            }
        case XLOG_SEG_CREATE_EXTENT_GROUP:
            SegPageRedoExtendGroup(&parseState->blockparse.blockhead);
            break;
        case XLOG_SEG_SPACE_SHRINK:
            SegPageRedoSpaceShrink(&parseState->blockparse.blockhead);
            break;
        case XLOG_SEG_NEW_PAGE:
            SegPageRedoNewPage(&parseState->blockparse.blockhead,
                &parseState->blockparse.extra_rec.blocksegnewpageinfo);
            break;
        case XLOG_SEG_SPACE_DROP:
            SegPageRedoSpaceDrop(&parseState->blockparse.blockhead);
            break;
        default:
            ereport(PANIC, (errmsg("ProcSegPageCommonRedo: unknown op code %u", info)));
            break;
    }
}

