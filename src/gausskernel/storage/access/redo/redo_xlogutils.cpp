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
 * xlogutils.cpp
 *    extreme rto entry
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/xlogutils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/nbtree.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "access/xlogproc.h"
#include "catalog/storage_xlog.h"
#include "access/visibilitymap.h"
#include "access/multi_redo_api.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "replication/catchup.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/ustore/knl_uextremeredo.h"

#include "commands/dbcommands.h"
#include "access/twophase.h"
#include "access/redo_common.h"
#include "access/extreme_rto/page_redo.h"

THR_LOCAL RedoParseManager *g_parseManager = NULL;
THR_LOCAL RedoBufferManager *g_bufferManager = NULL;

#ifdef BUILD_ALONE
THR_LOCAL bool assert_enabled = true;
#endif
const int XLOG_LSN_SWAP = 32;

static const ReadBufferMethod G_BUFFERREADMETHOD = WITH_NORMAL_CACHE;

bool ParseStateWithoutCache()
{
    return (G_BUFFERREADMETHOD == WITH_OUT_CACHE);
}

bool ParseStateUseLocalBuf()
{
    return (G_BUFFERREADMETHOD == WITH_LOCAL_CACHE);
}

bool ParseStateUseShareBuf()
{
    return (G_BUFFERREADMETHOD == WITH_NORMAL_CACHE);
}

static FORCE_INLINE bool XLogLsnCheckLogInvalidPage(const RedoBufferInfo *bufferinfo,
                                                    InvalidPageType type,
                                                    const XLogPhyBlock *pblk)
{
    log_invalid_page(bufferinfo->blockinfo.rnode, bufferinfo->blockinfo.forknum, bufferinfo->blockinfo.blkno,
            type, pblk);
    return false;
}

bool DoLsnCheck(const RedoBufferInfo *bufferinfo, bool willInit, XLogRecPtr lastLsn, const XLogPhyBlock *pblk,
    bool *needRepair)
{
    XLogRecPtr lsn = bufferinfo->lsn;
    Page page = (Page)bufferinfo->pageinfo.page;

    XLogRecPtr pageCurLsn = PageGetLSN(page);

    bool isSegmentPage = (IsSegmentFileNode(bufferinfo->blockinfo.rnode) &&
                                !IsSegmentPhysicalRelNode(bufferinfo->blockinfo.rnode));

    if (!(XLByteEQ(lastLsn, pageCurLsn))) {
        const RedoBufferTag *blockinfo = &(bufferinfo->blockinfo);

        if (willInit) {
            ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                             errmsg("new page lsn check error,lsn in record (%lu),lsn in current page %lu,"
                                    " page info:%u/%u/%u forknum %d lsn %lu blknum:%u",
                                    lastLsn, pageCurLsn, blockinfo->rnode.spcNode, blockinfo->rnode.dbNode,
                                    blockinfo->rnode.relNode, blockinfo->forknum, lsn, blockinfo->blkno)));
        } else if (lastLsn == InvalidXLogRecPtr) {
            ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                             errmsg("invalid lsn check error,lsn in record (%lu),lsn in current page %lu,"
                                    " page info:%u/%u/%u forknum %d lsn %lu blknum:%u",
                                    lastLsn, pageCurLsn, blockinfo->rnode.spcNode, blockinfo->rnode.dbNode,
                                    blockinfo->rnode.relNode, blockinfo->forknum, lsn, blockinfo->blkno)));
        } else if (PageIsAllVisible(page)) {
            ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                             errmsg("all visible page lsn check error,lsn in record (%lu),lsn in current page %lu,"
                                    " page info:%u/%u/%u forknum %d lsn %lu blknum:%u",
                                    lastLsn, pageCurLsn, blockinfo->rnode.spcNode, blockinfo->rnode.dbNode,
                                    blockinfo->rnode.relNode, blockinfo->forknum, lsn, blockinfo->blkno)));
        } else if (isSegmentPage) {
            return XLogLsnCheckLogInvalidPage(bufferinfo, SEGPAGE_LSN_CHECK_ERROR, pblk);
        } else if (PageIsJustAfterFullPageWrite(page)) {
            ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                             errmsg("after full page write lsn check error,lsn in record (%lu),"
                                    "lsn in current page %lu, page info:%u/%u/%u forknum %d lsn %lu blknum:%u",
                                    lastLsn, pageCurLsn, blockinfo->rnode.spcNode, blockinfo->rnode.dbNode,
                                    blockinfo->rnode.relNode, blockinfo->forknum, lsn, blockinfo->blkno)));
        } else if (pageCurLsn == InvalidXLogRecPtr && PageIsEmpty(page) && PageUpperIsInitNew(page)) {
            return XLogLsnCheckLogInvalidPage(bufferinfo, LSN_CHECK_ERROR, pblk);
        } else {
            int elevel = PANIC;
            if (CheckVerionSupportRepair() && IsPrimaryClusterStandbyDN() && g_instance.repair_cxt.support_repair) {
                elevel = WARNING;
                *needRepair = true;
                XLogLsnCheckLogInvalidPage(bufferinfo, LSN_CHECK_ERROR, pblk);
            }
            ereport(elevel,
                (errmsg("lsn check error, record last lsn (%X/%X) ,lsn in current page %X/%X, "
                        "page info:%u/%u/%u forknum %d blknum:%u lsn %X/%X",
                        (uint32)(lastLsn >> XLOG_LSN_SWAP), (uint32)(lastLsn), (uint32)(pageCurLsn >> XLOG_LSN_SWAP),
                        (uint32)(pageCurLsn), blockinfo->rnode.spcNode, blockinfo->rnode.dbNode,
                        blockinfo->rnode.relNode, blockinfo->forknum, blockinfo->blkno, (uint32)(lsn >> XLOG_LSN_SWAP),
                        (uint32)(lsn))));
            return false;
        }
    }
    return true;
}

char *XLogBlockDataGetBlockData(XLogBlockDataParse *datadecode, Size *len)
{
    if (!XLogBlockDataHasBlockData(datadecode))
        return NULL;

    if (len != NULL)
        *len = datadecode->blockdata.data_len;

    return datadecode->blockdata.data;
}

char *XLogBlockDataGetMainData(XLogBlockDataParse *datadecode, Size *len)
{
    if (len != NULL)
        *len = datadecode->main_data_len;

    return datadecode->main_data;
}

char *XLogBlockDataRecGetImage(XLogBlockDataParse *datadecode, uint16 *hole_offset, uint16 *hole_length)
{
    if (!XLogBlockDataHasBlockImage(datadecode))
        return NULL;

    if (hole_offset != NULL)
        *hole_offset = datadecode->blockdata.hole_offset;
    if (hole_length != NULL)
        *hole_length = datadecode->blockdata.hole_length;
    return datadecode->blockdata.bkp_image;
}

bool XLogBlockRefreshRedoBufferInfo(XLogBlockHead *blockhead, RedoBufferInfo *bufferinfo)
{
    /* todo: add debug info */
    if (bufferinfo->lsn > XLogBlockHeadGetLSN(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.rnode.spcNode != XLogBlockHeadGetSpcNode(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.rnode.dbNode != XLogBlockHeadGetDbNode(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.rnode.relNode != XLogBlockHeadGetRelNode(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.rnode.opt != XLogBlockHeadGetCompressOpt(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.forknum != XLogBlockHeadGetForkNum(blockhead)) {
        return false;
    }
    if (bufferinfo->blockinfo.blkno != XLogBlockHeadGetBlockNum(blockhead)) {
        return false;
    }

    if (bufferinfo->pageinfo.page == NULL && XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_DDL_TYPE &&
        XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_UNDO_TYPE) {
        return false;
    }

    bufferinfo->lsn = XLogBlockHeadGetLSN(blockhead);
    return true;
}

void XLogBlockInitRedoBlockInfo(XLogBlockHead *blockhead, RedoBufferTag *blockinfo)
{
    /* init blockinfo */
    blockinfo->rnode.spcNode = XLogBlockHeadGetSpcNode(blockhead);
    blockinfo->rnode.dbNode = XLogBlockHeadGetDbNode(blockhead);
    blockinfo->rnode.relNode = XLogBlockHeadGetRelNode(blockhead);
    blockinfo->rnode.bucketNode = XLogBlockHeadGetBucketId(blockhead);
    blockinfo->rnode.opt = XLogBlockHeadGetCompressOpt(blockhead);
    blockinfo->forknum = XLogBlockHeadGetForkNum(blockhead);
    blockinfo->blkno = XLogBlockHeadGetBlockNum(blockhead);
    blockinfo->pblk = XLogBlockHeadGetPhysicalBlock(blockhead);
}

void XlogUpdateFullPageWriteLsn(Page page, XLogRecPtr lsn)
{
    /*
     * The page may be uninitialized. If so, we can't set the LSN because
     * that would corrupt the page.
     */
    if (!PageIsNew(page)) {
        PageSetJustAfterFullPageWrite(page);
        PageSetLSN(page, lsn, false);
        PageClearLogical(page);
    }
}

XLogRedoAction XLogCheckBlockDataRedoAction(XLogBlockDataParse *datadecode, RedoBufferInfo *bufferinfo)
{
    if (XLogBlockDataHasBlockImage(datadecode)) {
        char *imagedata;
        uint16 hole_offset;
        uint16 hole_length;

        imagedata = XLogBlockDataRecGetImage(datadecode, &hole_offset, &hole_length);
        if (imagedata == NULL) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION), errmsg("XLogCheckRedoAction failed to restore block image")));
        } else {
            RestoreBlockImage(imagedata, hole_offset, hole_length, (char *)bufferinfo->pageinfo.page);
            XlogUpdateFullPageWriteLsn(bufferinfo->pageinfo.page, bufferinfo->lsn);
            MakeRedoBufferDirty(bufferinfo);
            return BLK_RESTORED;
        }
    } else {
        if (bufferinfo->pageinfo.page != NULL) {
            if (XLByteLE(bufferinfo->lsn, PageGetLSN(bufferinfo->pageinfo.page))) {
                return BLK_DONE;
            } else {
                if (EnalbeWalLsnCheck && bufferinfo->blockinfo.forknum == MAIN_FORKNUM) {
                    bool needRepair = false;
                    bool willinit = (XLogBlockDataGetBlockFlags(datadecode) & BKPBLOCK_WILL_INIT);
                    bool notSkip = DoLsnCheck(bufferinfo, willinit, XLogBlockDataGetLastBlockLSN(datadecode), 
                        (bufferinfo->blockinfo.pblk.relNode != InvalidOid) ? &bufferinfo->blockinfo.pblk : NULL,
                        &needRepair);
                    if (needRepair) {
                        XLogRecPtr pageCurLsn = PageGetLSN(bufferinfo->pageinfo.page);
                        UnlockReleaseBuffer(bufferinfo->buf);
                        extreme_rto::RecordBadBlockAndPushToRemote(datadecode, LSN_CHECK_FAIL, pageCurLsn,
                            bufferinfo->blockinfo.pblk);
                        bufferinfo->buf = InvalidBuffer;
                        bufferinfo->pageinfo = {0};
#ifdef USE_ASSERT_CHECKING
                        bufferinfo->pageinfo.ignorecheck = true;
#endif
                        return BLK_NOTFOUND;
                    }
                    if (!notSkip) {
                        return BLK_DONE;
                    }
                }
                PageClearJustAfterFullPageWrite(bufferinfo->pageinfo.page);
                return BLK_NEEDS_REDO;
            }
        }
    }
    return BLK_NOTFOUND;
}

void XLogRecSetBlockCommonState(XLogReaderState *record, XLogBlockParseEnum blockvalid, RelFileNodeForkNum filenode,
                                XLogRecParseState *recordblockstate, XLogPhyBlock *pblk)
{
    XLogBlockParse* blockparse = &(recordblockstate->blockparse);

    blockparse->blockhead.start_ptr = record->ReadRecPtr;
    blockparse->blockhead.end_ptr = record->EndRecPtr;
    blockparse->blockhead.block_valid = blockvalid;
    blockparse->blockhead.xl_info = XLogRecGetInfo(record);
    blockparse->blockhead.xl_rmid = XLogRecGetRmid(record);
    blockparse->blockhead.xl_xid = XLogRecGetXid(record);

    blockparse->blockhead.relNode = filenode.rnode.node.relNode;
    blockparse->blockhead.spcNode = filenode.rnode.node.spcNode;
    blockparse->blockhead.dbNode = filenode.rnode.node.dbNode;
    blockparse->blockhead.bucketNode = filenode.rnode.node.bucketNode;
    blockparse->blockhead.opt = filenode.rnode.node.opt;
    blockparse->blockhead.blkno = filenode.segno;
    blockparse->blockhead.forknum = filenode.forknumber;

    blockparse->redohead.xl_term = XLogRecGetTerm(record);
    
    if (pblk != NULL) {
        blockparse->blockhead.pblk = *pblk;
    } else {
        blockparse->blockhead.pblk.relNode = InvalidOid;
        blockparse->blockhead.pblk.block = InvalidBlockNumber;
        blockparse->blockhead.pblk.lsn = InvalidXLogRecPtr;
    }
}

#ifdef USE_ASSERT_CHECKING
void DoRecordCheck(XLogRecParseState *recordstate, XLogRecPtr pageLsn, bool replayed)
{
    if (recordstate->refrecord == NULL) {
        return;
    }

    if (recordstate->blockparse.blockhead.block_valid != BLOCK_DATA_MAIN_DATA_TYPE) {
        return;
    }

    RedoParseManager *manager = recordstate->manager;
    if (manager->refOperate != NULL) {
        manager->refOperate->checkFunc(recordstate->refrecord, pageLsn,
                                       recordstate->blockparse.extra_rec.blockdatarec.blockhead.cur_block_id, replayed);
    }
}
#endif

static void AddReadBlock(XLogRecParseState *recordstate, uint32 readblocks)
{
    if (recordstate->refrecord == NULL) {
        return;
    }

    if (recordstate->blockparse.blockhead.block_valid != BLOCK_DATA_MAIN_DATA_TYPE) {
        return;
    }

    RedoParseManager *manager = recordstate->manager;
    if (manager->refOperate != NULL) {
        manager->refOperate->addReadBlock(recordstate->refrecord, readblocks);
    }
}

static void DereferenceSrcRecord(RedoParseManager *parsemanager, void *record)
{
    Assert(parsemanager != NULL);
    if (parsemanager->refOperate != NULL) {
        parsemanager->refOperate->DerefCount(record);
    }
}

void XLogBlockParseStateRelease_debug(XLogRecParseState *recordstate, const char *func, uint32 line)
{
    if (recordstate == NULL)
        return;

    XLogRecParseState *nextstate = recordstate;
    do {
        XLogRecParseState *prev = nextstate;
        nextstate = (XLogRecParseState *)(nextstate->nextrecord);

        if (prev->refrecord) {
            DereferenceSrcRecord(prev->manager, prev->refrecord);
        }
        XLogParseBufferReleaseFunc(prev);
    } while (nextstate != NULL);
}

void XLogRecSetBlockDataStateContent(XLogReaderState *record, uint32 blockid, XLogBlockDataParse *blockdatarec)
{
    Assert(XLogRecHasBlockRef(record, blockid));
    DecodedBkpBlock *decodebkp = &(record->blocks[blockid]);

    blockdatarec->blockhead.auxiblk1 = InvalidBlockNumber;
    blockdatarec->blockhead.auxiblk2 = InvalidBlockNumber;
    blockdatarec->blockhead.cur_block_id = blockid;
    blockdatarec->blockhead.flags = decodebkp->flags;
    blockdatarec->blockhead.has_image = decodebkp->has_image;
    blockdatarec->blockhead.has_data = decodebkp->has_data;

    blockdatarec->blockdata.extra_flag = decodebkp->extra_flag;
    blockdatarec->blockdata.hole_offset = decodebkp->hole_offset;
    blockdatarec->blockdata.hole_length = decodebkp->hole_length;
    blockdatarec->blockdata.data_len = decodebkp->data_len;
    blockdatarec->blockdata.last_lsn = decodebkp->last_lsn;
    blockdatarec->blockdata.bkp_image = decodebkp->bkp_image;
    blockdatarec->blockdata.data = decodebkp->data;

    blockdatarec->main_data = XLogRecGetData(record);
    blockdatarec->main_data_len = XLogRecGetDataLen(record);
}

void XLogRecSetBlockDataState(XLogReaderState *record, uint32 blockid, XLogRecParseState *recordblockstate,
    XLogBlockParseEnum type)
{
    Assert(XLogRecHasBlockRef(record, blockid));
    DecodedBkpBlock *decodebkp = &(record->blocks[blockid]);
    XLogPhyBlock pblk;

    pblk.block = decodebkp->seg_blockno;
    pblk.relNode = decodebkp->seg_fileno;
    pblk.lsn = record->EndRecPtr;
    SegmentCheck(!XLOG_NEED_PHYSICAL_LOCATION(decodebkp->rnode) || PhyBlockIsValid(pblk));

    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(&decodebkp->rnode, InvalidBackendId, decodebkp->forknum, decodebkp->blkno);
    XLogRecSetBlockCommonState(record, type, filenode, recordblockstate, &pblk);

    XLogBlockDataParse *blockdatarec = &(recordblockstate->blockparse.extra_rec.blockdatarec);

    XLogRecSetBlockDataStateContent(record, blockid, blockdatarec);
}

void XLogRecSetAuxiBlkNumState(XLogBlockDataParse *blockdatarec, BlockNumber auxilaryblkn1, BlockNumber auxilaryblkn2)
{
    blockdatarec->blockhead.auxiblk1 = auxilaryblkn1;
    blockdatarec->blockhead.auxiblk2 = auxilaryblkn2;
}

void XLogRecSetVmBlockState(XLogReaderState *record, uint32 blockid, XLogRecParseState *recordblockstate)
{
    RelFileNode rnode;
    BlockNumber heapBlk;
    uint8 vmFile;
    BlockNumber vmBlkNo;

    bool has_vm_loc = false;
    Assert(XLogRecHasBlockRef(record, blockid));

    XLogRecGetBlockTag(record, blockid, &rnode, NULL, &heapBlk);
    XLogRecGetVMPhysicalBlock(record, blockid, &vmFile, &vmBlkNo, &has_vm_loc);

    XLogPhyBlock pblk;
    pblk.relNode = vmFile;
    pblk.block = vmBlkNo;
    pblk.lsn = record->EndRecPtr;

    SegmentCheck(!XLOG_NEED_PHYSICAL_LOCATION(rnode) || PhyBlockIsValid(pblk));
    BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, VISIBILITYMAP_FORKNUM, mapBlock);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_VM_TYPE, filenode, recordblockstate, has_vm_loc ? &pblk : NULL);

    XLogBlockVmParse *blockvm = &(recordblockstate->blockparse.extra_rec.blockvmrec);

    blockvm->heapBlk = heapBlk;

}

void GetXlUndoHeaderExtraData(char **currLogPtr, XlUndoHeaderExtra *xlundohdrextra, uint8 flag)
{
    xlundohdrextra->size = 0;

    if ((flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        xlundohdrextra->hasSubXact = *((bool *)*currLogPtr);
        xlundohdrextra->size += sizeof(bool);
        *currLogPtr += sizeof(bool);
    } else {
        xlundohdrextra->hasSubXact = false;
    }

    if ((flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        xlundohdrextra->blkprev = *((UndoRecPtr *)*currLogPtr);
        xlundohdrextra->size += sizeof(UndoRecPtr);
        *currLogPtr += sizeof(UndoRecPtr);
    } else {
        xlundohdrextra->blkprev = INVALID_UNDO_REC_PTR;
    }

    if ((flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        xlundohdrextra->prevurp = *((UndoRecPtr *)*currLogPtr);
        xlundohdrextra->size += sizeof(UndoRecPtr);
        *currLogPtr += sizeof(UndoRecPtr);
    } else {
        xlundohdrextra->prevurp = INVALID_UNDO_REC_PTR;
    }

    if ((flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        xlundohdrextra->partitionOid = *((Oid *)*currLogPtr);
        xlundohdrextra->size += sizeof(Oid);
        *currLogPtr += sizeof(Oid);
    } else {
        xlundohdrextra->partitionOid = 0;
    }
}

/* Set uheap undo insert block state for xlog record */
RelFileNode XLogRecSetUHeapUndoInsertBlockState(XLogReaderState *record,
    XlUHeapInsert *xlrec, insertUndoParse *parseBlock, DecodedBkpBlock *decodebkp)
{
    RelFileNode rnode;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert);
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    GetXlUndoHeaderExtraData(&currLogPtr, &parseBlock->xlundohdrextra, xlundohdr->flag);
    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);

    parseBlock->offnum = xlrec->offnum;
    parseBlock->recxid = XLogRecGetXid(record);
    parseBlock->blkno = decodebkp->blkno;
    parseBlock->spcNode = decodebkp->rnode.spcNode;
    parseBlock->relNode = decodebkp->rnode.relNode;
    parseBlock->lsn = record->EndRecPtr;
    parseBlock->xlundohdr.urecptr = xlundohdr->urecptr;
    parseBlock->xlundohdr.relOid = xlundohdr->relOid;

    undo::CopyUndoMeta(*xlundometa, parseBlock->xlundometa);
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, xlundohdr->urecptr, UNDO_DB_OID);

    return rnode;
}

RelFileNode XLogRecSetUHeapUndoDeleteBlockState(XLogReaderState *record,
    XlUHeapDelete *xlrec, XLogBlockUndoParse *blockundo, DecodedBkpBlock *decodebkp)
{
    RelFileNode rnode;
    Size recordlen = XLogRecGetDataLen(record);
    deleteUndoParse *parseBlock = &blockundo->deleteUndoParse;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete);
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    GetXlUndoHeaderExtraData(&currLogPtr, &parseBlock->xlundohdrextra, xlundohdr->flag);
    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
    uint32 undoMetaSize = xlundometa->Size();
    currLogPtr += undoMetaSize;

    blockundo->maindata = (char *)currLogPtr;
    blockundo->recordlen = recordlen - SizeOfUHeapDelete -
        SizeOfXLUndoHeader - parseBlock->xlundohdrextra.size -
        undoMetaSize - SizeOfUHeapHeader;

    parseBlock->recxid = XLogRecGetXid(record);
    parseBlock->offnum = xlrec->offnum;
    parseBlock->oldxid = xlrec->oldxid;
    parseBlock->blkno = decodebkp->blkno;
    parseBlock->spcNode = decodebkp->rnode.spcNode;
    parseBlock->relNode = decodebkp->rnode.relNode;
    parseBlock->lsn = record->EndRecPtr;
    parseBlock->xlundohdr.urecptr = xlundohdr->urecptr;
    parseBlock->xlundohdr.relOid = xlundohdr->relOid;

    undo::CopyUndoMeta(*xlundometa, parseBlock->xlundometa);
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, xlundohdr->urecptr, UNDO_DB_OID);

    return rnode;
}

RelFileNode XLogRecSetUHeapUndoUpdateBlockState(XLogReaderState *record,
    XlUHeapUpdate *xlrec, XLogBlockUndoParse *blockundo, DecodedBkpBlock *decodebkp)
{
    RelFileNode rnode;
    Size recordlen = XLogRecGetDataLen(record);
    updateUndoParse *parseBlock = &blockundo->updateUndoParse;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate);
    XlUndoHeader *xlnewundohdr;
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    GetXlUndoHeaderExtraData(&currLogPtr, &parseBlock->xlundohdrextra, xlundohdr->flag);

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        xlnewundohdr = (XlUndoHeader *)currLogPtr;
        currLogPtr += SizeOfXLUndoHeader;
        GetXlUndoHeaderExtraData(&currLogPtr, &parseBlock->xlnewundohdrextra, xlnewundohdr->flag);

        parseBlock->xlnewundohdr.urecptr = xlnewundohdr->urecptr;
        parseBlock->xlnewundohdr.relOid = xlnewundohdr->relOid;
    }

    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
    uint32 undoMetaSize = xlundometa->Size();
    currLogPtr += undoMetaSize;

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        Size initPageXtraInfo = 0;

        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
            /* has xidBase and tdCount */
            initPageXtraInfo = sizeof(TransactionId) + sizeof(uint16);
            currLogPtr += initPageXtraInfo;
        }

        blockundo->maindata = (char *)currLogPtr;
        blockundo->recordlen = recordlen - SizeOfUHeapUpdate -
            SizeOfXLUndoHeader - parseBlock->xlundohdrextra.size -
            SizeOfXLUndoHeader - parseBlock->xlnewundohdrextra.size -
            undoMetaSize - initPageXtraInfo - SizeOfUHeapHeader;
    } else {
        int *undoXorDeltaSizePtr = (int *)currLogPtr;
        parseBlock->undoXorDeltaSize = *undoXorDeltaSizePtr;
        currLogPtr += sizeof(int);
        parseBlock->xlogXorDelta = currLogPtr;
    }

    parseBlock->inplaceUpdate = !(xlrec->flags & XLZ_NON_INPLACE_UPDATE);
    parseBlock->recxid = XLogRecGetXid(record);
    parseBlock->oldxid = xlrec->oldxid;
    parseBlock->spcNode = decodebkp->rnode.spcNode;
    parseBlock->relNode = decodebkp->rnode.relNode;
    parseBlock->new_offnum = xlrec->new_offnum;
    parseBlock->old_offnum = xlrec->old_offnum;
    parseBlock->lsn = record->EndRecPtr;
    parseBlock->xlundohdr.urecptr = xlundohdr->urecptr;
    parseBlock->xlundohdr.relOid = xlundohdr->relOid;

    undo::CopyUndoMeta(*xlundometa, parseBlock->xlundometa);
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, xlundohdr->urecptr, UNDO_DB_OID);

    return rnode;
}

RelFileNode XLogRecSetUHeapUndoMultiInsertBlockState(XLogReaderState *record,
    XlUHeapMultiInsert *xlrec, XLogBlockUndoParse *blockundo, DecodedBkpBlock *decodebkp)
{
    RelFileNode rnode;
    multiInsertUndoParse *parseBlock = &blockundo->multiInsertUndoParse;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + 0);
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    GetXlUndoHeaderExtraData(&currLogPtr, &parseBlock->xlundohdrextra, xlundohdr->flag);

    UndoRecPtr *last_urecptr = (UndoRecPtr *)currLogPtr;
    currLogPtr = (char *)last_urecptr + sizeof(*last_urecptr);
    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
    currLogPtr += xlundometa->Size();

    if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
        /* has xidBase and tdCount */
        currLogPtr += sizeof(TransactionId) + sizeof(uint16);
    }

    blockundo->maindata = (char *)currLogPtr;
    xlrec = (XlUHeapMultiInsert *)((char *)currLogPtr);

    parseBlock->recxid = XLogRecGetXid(record);
    parseBlock->blkno = decodebkp->blkno;
    parseBlock->spcNode = decodebkp->rnode.spcNode;
    parseBlock->relNode = decodebkp->rnode.relNode;
    parseBlock->lsn = record->EndRecPtr;
    parseBlock->isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
    parseBlock->skipUndo = (xlrec->flags & XLZ_INSERT_IS_FROZEN);
    parseBlock->xlundohdr.urecptr = xlundohdr->urecptr;
    parseBlock->xlundohdr.relOid = xlundohdr->relOid;
    parseBlock->last_urecptr = *last_urecptr;

    undo::CopyUndoMeta(*xlundometa, parseBlock->xlundometa);
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, xlundohdr->urecptr, UNDO_DB_OID);

    return rnode;
}

void XLogRecSetUHeapUndoBlockState(XLogReaderState *record, uint32 blockid, XLogRecParseState *undostate)
{
    void *xlrec = (void *)XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    RelFileNode rnode;

    Assert(XLogRecHasBlockRef(record, blockid));
    DecodedBkpBlock *decodebkp = &(record->blocks[blockid]);

    XLogBlockUndoParse *blockundo = &(undostate->blockparse.extra_rec.blockundorec);

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT: {
            rnode = XLogRecSetUHeapUndoInsertBlockState(record, (XlUHeapInsert *)xlrec,
                &blockundo->insertUndoParse, decodebkp);
            break;
        }
        case XLOG_UHEAP_DELETE: {
            rnode = XLogRecSetUHeapUndoDeleteBlockState(record, (XlUHeapDelete *)xlrec,
                blockundo, decodebkp);
            break;
        }
        case XLOG_UHEAP_UPDATE: {
            rnode = XLogRecSetUHeapUndoUpdateBlockState(record, (XlUHeapUpdate *)xlrec,
                blockundo, decodebkp);
            break;
        }
        case XLOG_UHEAP_MULTI_INSERT: {
            rnode = XLogRecSetUHeapUndoMultiInsertBlockState(record, (XlUHeapMultiInsert *)xlrec,
                blockundo, decodebkp);
            break;
        }
        default:
            ereport(PANIC, (errmsg("XLogRecSetUHeapUndoBlockState: unknown op code %u", (uint8)info)));
    }

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, UNDO_FORKNUM, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_UNDO_TYPE, filenode, undostate);
}

void XLogRecSetUndoBlockState(XLogReaderState *record, uint32 blockid, XLogRecParseState *undostate)
{
    XLogBlockUndoParse *blockundo = &(undostate->blockparse.extra_rec.blockundorec);

    void *xlrec = (void *)XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int zoneId = 0;

    switch (info) {
        case XLOG_UNDO_DISCARD:{
            undo::XlogUndoDiscard *xlrecDiscard = (undo::XlogUndoDiscard *)xlrec;
            blockundo->undoDiscardParse.zoneId = UNDO_PTR_GET_ZONE_ID(xlrecDiscard->startSlot);
            blockundo->undoDiscardParse.endSlot = xlrecDiscard->endSlot;
            blockundo->undoDiscardParse.startSlot = xlrecDiscard->startSlot;
            blockundo->undoDiscardParse.endUndoPtr = xlrecDiscard->endUndoPtr;
            blockundo->undoDiscardParse.recycledXid = xlrecDiscard->recycledXid;
            blockundo->undoDiscardParse.lsn = record->EndRecPtr;
            zoneId = blockundo->undoDiscardParse.zoneId;
            break;
        }
        case XLOG_UNDO_UNLINK: {
            UndoRecPtr headPtr = ((undo::XlogUndoUnlink *)xlrec)->head;
            blockundo->undoUnlinkParse.zoneId = UNDO_PTR_GET_ZONE_ID(headPtr);
            blockundo->undoUnlinkParse.headOffset = UNDO_PTR_GET_OFFSET(headPtr);
            blockundo->undoUnlinkParse.unlinkLsn = record->EndRecPtr;
            zoneId = blockundo->undoUnlinkParse.zoneId;
            break;
        }
        case XLOG_SLOT_UNLINK: {
            UndoRecPtr headPtr = ((undo::XlogUndoUnlink *)xlrec)->head;
            blockundo->undoUnlinkParse.zoneId = UNDO_PTR_GET_ZONE_ID(headPtr);
            blockundo->undoUnlinkParse.headOffset = UNDO_PTR_GET_OFFSET(headPtr);
            blockundo->undoUnlinkParse.unlinkLsn = record->EndRecPtr;
            zoneId = blockundo->undoUnlinkParse.zoneId;
            break;
        }
        case XLOG_UNDO_EXTEND: {
            UndoRecPtr tailPtr = ((undo::XlogUndoExtend *)xlrec)->tail;
            blockundo->undoExtendParse.zoneId = UNDO_PTR_GET_ZONE_ID(tailPtr);
            blockundo->undoExtendParse.tailOffset = UNDO_PTR_GET_OFFSET(tailPtr);
            blockundo->undoExtendParse.extendLsn = record->EndRecPtr;
            zoneId = blockundo->undoExtendParse.zoneId;
            break;
        }
        case XLOG_SLOT_EXTEND: {
            UndoRecPtr tailPtr = ((undo::XlogUndoExtend *)xlrec)->tail;
            blockundo->undoExtendParse.zoneId = UNDO_PTR_GET_ZONE_ID(tailPtr);
            blockundo->undoExtendParse.tailOffset = UNDO_PTR_GET_OFFSET(tailPtr);
            blockundo->undoExtendParse.extendLsn = record->EndRecPtr;
            zoneId = blockundo->undoExtendParse.zoneId;
            break;
        }
        case XLOG_UNDO_CLEAN: {
            UndoRecPtr tailPtr = ((undo::XlogUndoClean *)xlrec)->tail;
            blockundo->undoCleanParse.zoneId = UNDO_PTR_GET_ZONE_ID(tailPtr);
            blockundo->undoCleanParse.tailOffset = UNDO_PTR_GET_OFFSET(tailPtr);
            blockundo->undoCleanParse.cleanLsn = record->EndRecPtr;
            zoneId = blockundo->undoCleanParse.zoneId;
            break;
        }
        case XLOG_SLOT_CLEAN: {
            UndoRecPtr tailPtr = ((undo::XlogUndoClean *)xlrec)->tail;
            blockundo->undoCleanParse.zoneId = UNDO_PTR_GET_ZONE_ID(tailPtr);
            blockundo->undoCleanParse.tailOffset = UNDO_PTR_GET_OFFSET(tailPtr);
            blockundo->undoCleanParse.cleanLsn = record->EndRecPtr;
            zoneId = blockundo->undoCleanParse.zoneId;
            break;
        }
        default:
            ereport(PANIC, (errmsg("XLogRecSetUndoBlockState: unknown op code %u", (uint8)info)));
    }

    RelFileNode rnode = { DEFAULTTABLESPACE_OID, UNDO_DB_OID, (Oid)zoneId, InvalidBktId };
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, UNDO_FORKNUM, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_UNDO_TYPE, filenode, undostate);
}

void XLogRecSetRollbackFinishBlockState(XLogReaderState *record, uint32 blockid, XLogRecParseState *undostate)
{
    XLogBlockUndoParse *blockundo = &(undostate->blockparse.extra_rec.blockundorec);
    undo::XlogRollbackFinish *xlrec = (undo::XlogRollbackFinish *)XLogRecGetData(record);

    RelFileNode rnode = {
        DEFAULTTABLESPACE_OID, UNDO_DB_OID, (Oid)(UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr)), InvalidBktId
    };
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, UNDO_FORKNUM, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_UNDO_TYPE, filenode, undostate);

    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_ROLLBACK_FINISH: {
            blockundo->rollbackFinishParse.slotPtr = xlrec->slotPtr;
            blockundo->rollbackFinishParse.lsn = record->EndRecPtr;
            break;
        }
        default:
            ereport(PANIC, (errmsg("XLogRecSetRollbackFinishBlockState: unknown op code %u", (uint8)info)));
    }
}

void XLogUpdateCopyedBlockState(XLogRecParseState *recordblockstate, XLogBlockParseEnum blockvalid, Oid spcnode,
                                Oid dbnode, Oid relid, int4 bucketNode, BlockNumber blkno, ForkNumber forknum)
{
    recordblockstate->blockparse.blockhead.block_valid = blockvalid;
    recordblockstate->blockparse.blockhead.spcNode = spcnode;
    recordblockstate->blockparse.blockhead.dbNode = dbnode;
    recordblockstate->blockparse.blockhead.relNode = relid;
    recordblockstate->blockparse.blockhead.blkno = blkno;
    recordblockstate->blockparse.blockhead.forknum = forknum;
    recordblockstate->blockparse.blockhead.bucketNode = bucketNode;
}

void XLogRecSetBlockDdlState(XLogBlockDdlParse *blockddlstate, uint32 blockddltype, char *mainData, int rels)
{
    Assert(blockddlstate != NULL);
    blockddlstate->blockddltype = blockddltype;
    blockddlstate->rels = rels;
    blockddlstate->mainData = mainData;
}

void XLogRecSetBlockCLogState(XLogBlockCLogParse *blockclogstate, TransactionId topxid, uint16 status, uint16 xidnum,
                              uint16 *xidsarry)
{
    blockclogstate->topxid = topxid;
    blockclogstate->status = status;
    blockclogstate->xidnum = xidnum;

    for (int i = 0; i < xidnum; i++) {
        blockclogstate->xidsarry[i] = xidsarry[i];
    }
}

void XLogRecSetBlockCSNLogState(XLogBlockCSNLogParse *blockcsnlogstate, TransactionId topxid, CommitSeqNo csnseq,
                                uint16 xidnum, uint16 *xidsarry)
{
    blockcsnlogstate->topxid = topxid;
    blockcsnlogstate->cslseq = csnseq;
    blockcsnlogstate->xidnum = xidnum;

    for (int i = 0; i < xidnum; i++) {
        blockcsnlogstate->xidsarry[i] = xidsarry[i];
    }
}

void XLogRecSetXactRecoveryState(XLogBlockXactParse *blockxactstate, TransactionId maxxid, CommitSeqNo maxcsnseq,
                                 uint8 delayddlflag, uint8 updateminrecovery)
{
    blockxactstate->maxxid = maxxid;
    blockxactstate->maxcommitseq = maxcsnseq;
    blockxactstate->delayddlflag = delayddlflag;
    blockxactstate->updateminrecovery = updateminrecovery;
}

void XLogRecSetXactDdlState(XLogBlockXactParse *blockxactstate, int nrels, void *xnodes, int invalidmsgnum,
                            void *invalidmsg, int nlibs, void *libfilename)
{
    blockxactstate->nrels = nrels;
    blockxactstate->xnodes = xnodes;
    blockxactstate->invalidmsgnum = invalidmsgnum;
    blockxactstate->invalidmsg = invalidmsg;
    blockxactstate->nlibs = nlibs;
    blockxactstate->libfilename = libfilename;
}

void XLogRecSetXactCommonState(XLogBlockXactParse *blockxactstate, uint16 committype, uint64 xinfo,
                               TimestampTz xact_time)
{
    blockxactstate->committype = committype;
    blockxactstate->xinfo = xinfo;
    blockxactstate->xact_time = xact_time;
}

void XLogRecSetBcmState(XLogBlockBcmParse *blockbcmrec, uint64 startblock, int count, int status)
{
    blockbcmrec->startblock = startblock;
    blockbcmrec->count = count;
    blockbcmrec->status = status;
}

void XLogRecSetNewCuState(XLogBlockNewCuParse *blockcudata, char *main_data, uint32 main_data_len)
{
    blockcudata->main_data = main_data;
    blockcudata->main_data_len = main_data_len;
}

void XLogRecSetInvalidMsgState(XLogBlockInvalidParse *blockinvalid, TransactionId cutoffxid)
{
    blockinvalid->cutoffxid = cutoffxid;
}

void XLogRecSetIncompleteMsgState(XLogBlockIncompleteParse *blockincomplete, uint16 action, bool issplit, bool isroot,
                                  BlockNumber downblk, BlockNumber leftblk, BlockNumber rightblk)
{
    blockincomplete->action = action;
    blockincomplete->issplit = issplit;
    blockincomplete->isroot = isroot;
    blockincomplete->downblk = downblk;
    blockincomplete->leftblk = leftblk;
    blockincomplete->rightblk = rightblk;
}

void XLogRecSetPinVacuumState(XLogBlockVacuumPinParse *blockvacuum, BlockNumber lastblknum)
{
    blockvacuum->lastBlockVacuumed = lastblknum;
}

void XLogRecSetSegFullSyncState(XLogBlockSegFullSyncParse *state, void *childState)
{
    Assert(state != NULL);
    state->childState = childState;
}

void XLogRecSetSegNewPageInfo(XLogBlockSegNewPage *state, char *mainData, Size len)
{
    Assert(state != NULL);
    state->mainData = mainData;
    state->dataLen = len;
}


static inline bool AtomicCompareExchangeBuffer(volatile Buffer *ptr, Buffer *expected, Buffer newval)
{
    bool ret = false;
    Buffer current;
    current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = (current == *expected);
    *expected = current;
    return ret;
}

static inline Buffer AtomicReadBuffer(volatile Buffer *ptr)
{
    return *ptr;
}

static inline void AtomicWriteBuffer(volatile Buffer* ptr, Buffer val)
{
    *ptr = val;
}

static inline Buffer AtomicExchangeBuffer(volatile Buffer *ptr, Buffer newval)
{
    Buffer old;
    while (true) {
        old = AtomicReadBuffer(ptr);
        if (AtomicCompareExchangeBuffer(ptr, &old, newval))
            break;
    }
    return old;
}

/* add for batch redo mem manager */
void *XLogMemCtlInit(RedoMemManager *memctl, Size itemsize, int itemnum)
{
    void *allocdata = NULL;
    RedoMemSlot *nextfreeslot = NULL;
    allocdata = (void *)palloc((itemsize + sizeof(RedoMemSlot)) * itemnum);
    if (allocdata == NULL) {
        ereport(PANIC,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("XLogMemCtlInit Allocated buffer failed!, taoalblknum:%d, itemsize:%lu", itemnum, itemsize)));
        /* panic */
    }

    Size dataSize = (itemsize + sizeof(RedoMemSlot)) * itemnum;
    errno_t rc = memset_s(allocdata, dataSize, 0, dataSize);
    securec_check(rc, "\0", "\0");
    memctl->totalblknum = itemnum;
    memctl->usedblknum = 0;
    memctl->itemsize = itemsize;
    memctl->memslot = (RedoMemSlot *)((char *)allocdata + (itemsize * itemnum));
    nextfreeslot = memctl->memslot;
    for (int i = memctl->totalblknum; i > 0; --i) {
        memctl->memslot[i - 1].buf_id = i; /*  start from 1 , 0 is invalidbuffer */
        memctl->memslot[i - 1].freeNext = i - 1;
    }
    memctl->firstfreeslot = memctl->totalblknum;
    memctl->firstreleaseslot = InvalidBuffer;
    return allocdata;
}

RedoMemSlot *XLogMemAlloc(RedoMemManager *memctl)
{
    RedoMemSlot *nextfreeslot = NULL;
    do {
        if (memctl->firstfreeslot == InvalidBuffer) {
            memctl->firstfreeslot = AtomicExchangeBuffer(&memctl->firstreleaseslot, InvalidBuffer);
            pg_read_barrier();
        }

        if (memctl->firstfreeslot != InvalidBuffer) {
            nextfreeslot = &(memctl->memslot[memctl->firstfreeslot - 1]);
            memctl->firstfreeslot = nextfreeslot->freeNext;
            memctl->usedblknum++;
            nextfreeslot->freeNext = InvalidBuffer;
        }

        if (memctl->doInterrupt != NULL) {
            memctl->doInterrupt();
        }

    } while (nextfreeslot == NULL);

    return nextfreeslot;
}

void XLogMemRelease(RedoMemManager *memctl, Buffer bufferid)
{
    RedoMemSlot *bufferslot;
    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogMemRelease failed!, taoalblknum:%u, buf_id:%u", memctl->totalblknum, bufferid)));
        /* panic */
    }
    bufferslot = &(memctl->memslot[bufferid - 1]);
    Assert(bufferslot->freeNext == InvalidBuffer);
    Buffer oldFirst = AtomicReadBuffer(&memctl->firstreleaseslot);
    pg_memory_barrier();
    do {
        AtomicWriteBuffer(&bufferslot->freeNext, oldFirst);
    } while (!AtomicCompareExchangeBuffer(&memctl->firstreleaseslot, &oldFirst, bufferid));
}

void XLogRedoBufferInit(RedoBufferManager *buffermanager, int buffernum, RefOperate *refOperate,
                        InterruptFunc interruptOperte)
{
    void *allocdata = XLogMemCtlInit(&(buffermanager->memctl), (BLCKSZ + sizeof(RedoBufferDesc)), buffernum);
    buffermanager->BufferBlockPointers = allocdata;
    buffermanager->refOperate = refOperate;
    buffermanager->memctl.doInterrupt = interruptOperte;
    buffermanager->memctl.isInit = true;
    g_bufferManager = buffermanager;
    return;
}

void XLogRedoBufferDestory(RedoBufferManager *buffermanager)
{
    g_bufferManager = NULL;
    buffermanager->memctl.isInit = false;
    if (buffermanager->BufferBlockPointers != NULL) {
        pfree(buffermanager->BufferBlockPointers);
        buffermanager->BufferBlockPointers = NULL;
    }
}

RedoMemSlot *XLogRedoBufferAlloc(RedoBufferManager *buffermanager, RelFileNode relnode, ForkNumber forkNum,
                                 BlockNumber blockNum)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoMemSlot *allocslot = NULL;
    RedoBufferDesc *buffdsc = NULL;

    allocslot = XLogMemAlloc(memctl);
    if (allocslot == NULL) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogRedoBufferAlloc Allocated buffer failed!, taoalblknum:%u, usedblknum:%u",
                               memctl->totalblknum, memctl->usedblknum)));
        /* panic */
    }
    Assert(allocslot->buf_id != InvalidBuffer);
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    buffdsc =
        (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (allocslot->buf_id - 1));
    buffdsc->blockinfo.rnode = relnode;
    buffdsc->blockinfo.forknum = forkNum;
    buffdsc->blockinfo.blkno = blockNum;
    buffdsc->state = BM_TAG_VALID;
    return allocslot;
}

bool XLogRedoBufferIsValid(RedoBufferManager *buffermanager, Buffer bufferid)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    if (!RedoMemIsValid(memctl, bufferid)) {
        return false;
    }
    return true;
}
void XLogRedoBufferRelease(RedoBufferManager *buffermanager, Buffer bufferid)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoBufferDesc *bufferdesc = NULL;

    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("XLogRedoBufferRelease failed!, taoalblknum:%u, buf_id:%u", memctl->totalblknum, bufferid)));
        /* panic */
    }
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    bufferdesc = (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (bufferid - 1));
    Assert(bufferdesc->state != 0);
    bufferdesc->state = 0;
    XLogMemRelease(memctl, bufferid);
}

BlockNumber XLogRedoBufferGetBlkNumber(RedoBufferManager *buffermanager, Buffer bufferid)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoBufferDesc *bufferdesc = NULL;

    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogRedoBufferGetBlkNumber get bufferblknum failed!, taoalblknum:%u, buf_id:%u",
                               memctl->totalblknum, bufferid)));
        /* panic */
    }
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    bufferdesc = (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (bufferid - 1));

    Assert(bufferdesc->state & BM_TAG_VALID);

    return bufferdesc->blockinfo.blkno;
}

Block XLogRedoBufferGetBlk(RedoBufferManager *buffermanager, RedoMemSlot *bufferslot)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoBufferDesc *bufferdesc;
    if (!RedoMemIsValid(memctl, bufferslot->buf_id)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogRedoBufferGetBlk get bufferblk failed!, taoalblknum:%u, buf_id:%u",
                               memctl->totalblknum, bufferslot->buf_id)));
        /* panic */
    }
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    bufferdesc =
        (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (bufferslot->buf_id - 1));
    Assert(bufferdesc->state & BM_TAG_VALID);
    Block blkdata = (Block)((char *)bufferdesc + sizeof(RedoBufferDesc));
    return blkdata;
}

Block XLogRedoBufferGetPage(RedoBufferManager *buffermanager, Buffer bufferid)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoBufferDesc *bufferdesc;
    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogRedoBufferGetPage get bufferblk failed!, taoalblknum:%u, buf_id:%u",
                               memctl->totalblknum, bufferid)));
        /* panic */
    }
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    bufferdesc = (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (bufferid - 1));
    Assert(bufferdesc->state & BM_VALID);
    Block blkdata = (Block)((char *)bufferdesc + sizeof(RedoBufferDesc));
    return blkdata;
}

void XLogRedoBufferSetState(RedoBufferManager *buffermanager, RedoMemSlot *bufferslot, uint32 state)
{
    RedoMemManager *memctl = &(buffermanager->memctl);
    RedoBufferDesc *bufferdesc = NULL;
    if (!RedoMemIsValid(memctl, bufferslot->buf_id)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogRedoBufferSetState get bufferblk failed!, taoalblknum:%u, buf_id:%u",
                               memctl->totalblknum, bufferslot->buf_id)));
        /* panic */
    }
    Assert(memctl->itemsize == (BLCKSZ + sizeof(RedoBufferDesc)));
    bufferdesc =
        (RedoBufferDesc *)((char *)buffermanager->BufferBlockPointers + memctl->itemsize * (bufferslot->buf_id - 1));
    bufferdesc->state |= state;
}

void XLogParseBufferInit(RedoParseManager *parsemanager, int buffernum, RefOperate *refOperate,
                         InterruptFunc interruptOperte)
{
    void *allocdata = NULL;
    allocdata = XLogMemCtlInit(&(parsemanager->memctl), (sizeof(XLogRecParseState) + sizeof(ParseBufferDesc)),
                               buffernum);
    parsemanager->parsebuffers = allocdata;
    parsemanager->refOperate = refOperate;
    parsemanager->memctl.doInterrupt = interruptOperte;
    parsemanager->memctl.isInit = true;

    g_parseManager = parsemanager;
    return;
}

void XLogParseBufferDestory(RedoParseManager *parsemanager)
{
    g_parseManager = NULL;
    if (parsemanager->parsebuffers != NULL) {
        pfree(parsemanager->parsebuffers);
        parsemanager->parsebuffers = NULL;
    }
    parsemanager->memctl.isInit = false;
}

XLogRecParseState *XLogParseBufferAllocList(RedoParseManager *parsemanager, XLogRecParseState *blkstatehead,
                                            void *record)
{
    RedoMemManager *memctl = &(parsemanager->memctl);
    RedoMemSlot *allocslot = NULL;
    ParseBufferDesc *descstate = NULL;
    XLogRecParseState *recordstate = NULL;

    allocslot = XLogMemAlloc(memctl);
    if (allocslot == NULL) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("XLogParseBufferAlloc Allocated buffer failed!, taoalblknum:%u, usedblknum:%u",
                                 memctl->totalblknum, memctl->usedblknum)));
        return NULL;
    }

    pg_read_barrier();
    Assert(allocslot->buf_id != InvalidBuffer);
    Assert(memctl->itemsize == (sizeof(XLogRecParseState) + sizeof(ParseBufferDesc)));
    descstate = (ParseBufferDesc *)((char *)parsemanager->parsebuffers + memctl->itemsize * (allocslot->buf_id - 1));
    descstate->buff_id = allocslot->buf_id;
    Assert(descstate->state == 0);
    descstate->state = 1;
    recordstate = (XLogRecParseState *)((char *)descstate + sizeof(ParseBufferDesc));
    recordstate->nextrecord = NULL;
    recordstate->manager = parsemanager;
    recordstate->refrecord = record;
    recordstate->isFullSync = false;
    if (blkstatehead != NULL) {
        recordstate->nextrecord = blkstatehead->nextrecord;
        blkstatehead->nextrecord = (void *)recordstate;
    }

    if (parsemanager->refOperate != NULL)
        parsemanager->refOperate->refCount(record);

    return recordstate;
}

XLogRecParseState *XLogParseBufferCopy(XLogRecParseState *srcState)
{
    XLogRecParseState *newState = NULL;
    XLogParseBufferAllocListFunc(srcState->refrecord, &newState, NULL);
    errno_t rc = memcpy_s(&newState->blockparse, sizeof(newState->blockparse), &srcState->blockparse,
                          sizeof(srcState->blockparse));
    securec_check(rc, "\0", "\0");

    newState->isFullSync = srcState->isFullSync;
    return newState;
}

void XLogParseBufferRelease(XLogRecParseState *recordstate)
{
    RedoMemManager *memctl = &(recordstate->manager->memctl);
    ParseBufferDesc *descstate = NULL;

    descstate = (ParseBufferDesc *)((char *)recordstate - sizeof(ParseBufferDesc));
    if (!RedoMemIsValid(memctl, descstate->buff_id) || descstate->state == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogParseBufferRelease failed!, taoalblknum:%u, buf_id:%u", memctl->totalblknum,
                               descstate->buff_id)));
        /* panic */
    }

    descstate->state = 0;

    XLogMemRelease(memctl, descstate->buff_id);
}

void XLogBlockDataCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
    if (XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_MAIN_DATA_TYPE) {
        ereport(PANIC, (errmsg("XLogBlockDataCommonRedo: redobuffer checkfailed")));
    }
    XLogBlockDataParse *blockdatarec = (XLogBlockDataParse *)blockrecbody;
    RmgrId rmid = XLogBlockHeadGetRmid(blockhead);
    switch (rmid) {
        case RM_HEAP_ID:
            HeapRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_HEAP2_ID:
            Heap2RedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_HEAP3_ID:
            Heap3RedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_BTREE_ID:
            BtreeRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_UBTREE_ID:
            UBTreeRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_UBTREE2_ID:
            UBTree2RedoDataBlock(blockhead, blockdatarec, bufferinfo);
        case RM_HASH_ID:
            HashRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_XLOG_ID:
            xlog_redo_data_block(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_GIN_ID:
            GinRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_GIST_ID:
            GistRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_SPGIST_ID:
            break;
        case RM_SEQ_ID:
            seq_redo_data_block(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_UHEAP_ID:
            UHeapRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_UHEAP2_ID:
            UHeap2RedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_UHEAPUNDO_ID:
            RedoUndoActionBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case RM_SEGPAGE_ID:
            SegPageRedoDataBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("XLogBlockDataCommonRedo: unknown rmid %u", rmid)));
    }
}

void XLogBlockVmCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
    if (XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_VM_TYPE) {
        ereport(PANIC, (errmsg("XLogBlockVmCommonRedo: redobuffer checkfailed")));
    }

    XLogBlockVmParse *blockvmrec = (XLogBlockVmParse *)blockrecbody;

    RmgrId rmid = XLogBlockHeadGetRmid(blockhead);
    switch (rmid) {
        case RM_HEAP_ID:
            HeapRedoVmBlock(blockhead, blockvmrec, bufferinfo);
            break;
        case RM_HEAP2_ID:
            Heap2RedoVmBlock(blockhead, blockvmrec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("XLogBlockVmCommonRedo: unknown rmid %u", rmid)));
    }
}

void XLogBlockUndoCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
    if (XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_UNDO_TYPE) {
        ereport(PANIC, (errmsg("XLogBlockUndoCommonRedo: redobuffer checkfailed")));
    }

    XLogBlockUndoParse *blockundorec = (XLogBlockUndoParse *)blockrecbody;

    RmgrId rmid = XLogBlockHeadGetRmid(blockhead);
    switch (rmid) {
        case RM_UHEAP_ID:
            RedoUHeapUndoBlock(blockhead, blockundorec, bufferinfo);
            break;
        case RM_UNDOLOG_ID:
            RedoUndoBlock(blockhead, blockundorec, bufferinfo);
            break;
        case RM_UNDOACTION_ID:
            RedoRollbackFinishBlock(blockhead, blockundorec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("XLogBlockUndoCommonRedo: unknown rmid %u", rmid)));
    }
}

void XLogBlockFsmCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
}

void XLogBlockDdlTruncateRedo(RelFileNode rnode, BlockNumber blkno)
{
    FSMAddress first_removed_address;
    uint16 first_removed_slot;
    BlockNumber new_nfsmblocks;

    XLogTruncateRelation(rnode, MAIN_FORKNUM, blkno);
    /* Get the location in the FSM of the first removed heap block */
    first_removed_address = fsm_get_location(blkno, &first_removed_slot);

    if (first_removed_slot > 0) {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
    }
    XLogTruncateRelation(rnode, FSM_FORKNUM, new_nfsmblocks);

    BlockNumber newnblocks;
    BlockNumber truncBlock = HEAPBLK_TO_MAPBLOCK(blkno);
    uint32 truncByte = HEAPBLK_TO_MAPBYTE(blkno);
    uint8 truncBit = HEAPBLK_TO_MAPBIT(blkno);
    if (truncByte != 0 || truncBit != 0) {
        newnblocks = truncBlock + 1;
    } else {
        newnblocks = truncBlock;
    }
    XLogTruncateRelation(rnode, VISIBILITYMAP_FORKNUM, newnblocks);

    /* delete from local hash */
    if (G_BUFFERREADMETHOD == WITH_LOCAL_CACHE) {
        DropRelFileNodeLocalBuffers(rnode, MAIN_FORKNUM, blkno);
        DropRelFileNodeLocalBuffers(rnode, FSM_FORKNUM, new_nfsmblocks);
        DropRelFileNodeLocalBuffers(rnode, VISIBILITYMAP_FORKNUM, newnblocks);
    } else if (G_BUFFERREADMETHOD == WITH_NORMAL_CACHE) {
        DropRelFileNodeShareBuffers(rnode, MAIN_FORKNUM, blkno);
        DropRelFileNodeShareBuffers(rnode, FSM_FORKNUM, new_nfsmblocks);
        DropRelFileNodeShareBuffers(rnode, VISIBILITYMAP_FORKNUM, newnblocks);
    }
}
void XLogBlockDdlCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
    XLogBlockDdlParse *blockddlrec = (XLogBlockDdlParse *)blockrecbody;

    RelFileNode rnode;
    rnode.spcNode = blockhead->spcNode;
    rnode.dbNode = blockhead->dbNode;
    rnode.relNode = blockhead->relNode;
    rnode.bucketNode = blockhead->bucketNode;
    rnode.opt = blockhead->opt;
    switch (blockddlrec->blockddltype) {
        case BLOCK_DDL_CREATE_RELNODE:
            smgr_redo_create(rnode, blockhead->forknum, blockddlrec->mainData);
            break;
        case BLOCK_DDL_TRUNCATE_RELNODE:
            XLogBlockDdlTruncateRedo(rnode, blockhead->blkno);
            break;
        default:
            break;
    }
}

void xlog_block_segpage_redo_truncate(RelFileNode rnode, XLogBlockHead *blockhead, XLogBlockSegDdlParse* segddlrec)
{
    XLogRedoAction redoaction;
    bool willinit;

    RedoBufferInfo bufferinfo = {0};
    XLogBlockInitRedoBlockInfo(blockhead, &bufferinfo.blockinfo);
    
    XLogBlockDataParse *blockdatarec = (XLogBlockDataParse *)&(segddlrec->blockddlrec);
    XLogRecPtr xlogLsn = XLogBlockHeadGetLSN(blockhead);
    
    willinit = XLogBlockDataGetBlockFlags(blockdatarec) & BKPBLOCK_WILL_INIT;
    ReadBufferMode mode = RBM_NORMAL;
    if (willinit || blockdatarec->blockhead.has_image) {
        mode = RBM_ZERO_AND_LOCK;
    } else if (blockhead->forknum > MAIN_FORKNUM) {
        mode = RBM_ZERO_ON_ERROR;
    }

    redoaction = XLogReadBufferForRedoBlockExtend(&bufferinfo.blockinfo, mode, false, &bufferinfo, xlogLsn,
                                                  InvalidXLogRecPtr, willinit, WITH_NORMAL_CACHE);
    if (redoaction == BLK_NOTFOUND) {
        return;
    }

    bool checkvalid = XLogBlockRefreshRedoBufferInfo(blockhead, &bufferinfo);
    if (!checkvalid) {
        ereport(PANIC, (errmsg("XLogBlockRedoForExtremeRTO: redobuffer checkfailed")));
    }

    redoaction = XLogCheckBlockDataRedoAction(blockdatarec, &bufferinfo);
    if (redoaction == BLK_NEEDS_REDO) {
        BlockNumber nblocks = *(BlockNumber *)XLogBlockDataGetBlockData(blockdatarec, NULL);
        
        Page page = bufferinfo.pageinfo.page;
        SegmentHead *seg_head = (SegmentHead *)PageGetContents(page);
        seg_head->nblocks = nblocks;
        PageSetLSN(page, bufferinfo.lsn);
        
        SegMarkBufferDirty(bufferinfo.buf);
    }

    if (BufferIsValid(bufferinfo.buf)) {
        SegUnlockReleaseBuffer(bufferinfo.buf);
    }
}

void XLogBlockSegDdlDoRealAction(XLogBlockHead* blockhead, void* blockrecbody, RedoBufferInfo* bufferinfo)
{
    XLogBlockSegDdlParse* segddlrec = (XLogBlockSegDdlParse*)blockrecbody;

    RelFileNode rnode;
    rnode.spcNode = blockhead->spcNode;
    rnode.dbNode = blockhead->dbNode;
    rnode.relNode = blockhead->relNode;
    rnode.bucketNode = blockhead->bucketNode;
    rnode.opt = blockhead->opt;
    switch (segddlrec->blockddlrec.blockddltype) {
        case BLOCK_DDL_TRUNCATE_RELNODE:
            xlog_block_segpage_redo_truncate(rnode, blockhead, segddlrec);
            break;
        case BLOCK_DDL_DROP_RELNODE: {
            SMgrRelation reln =
                smgropen(bufferinfo->blockinfo.rnode, InvalidBackendId, GetColumnNum(bufferinfo->blockinfo.forknum));
            smgrclose(reln);
            break;
        }
        default:
            break;
    }
}

void XLogBlockDdlDoSmgrAction(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
    XLogBlockDdlParse *blockddlrec = (XLogBlockDdlParse *)blockrecbody;

    RelFileNode rnode;
    rnode.spcNode = blockhead->spcNode;
    rnode.dbNode = blockhead->dbNode;
    rnode.relNode = blockhead->relNode;
    rnode.bucketNode = blockhead->bucketNode;
    rnode.opt = blockhead->opt;
    switch (blockddlrec->blockddltype) {
        case BLOCK_DDL_CREATE_RELNODE:
            smgr_redo_create(rnode, blockhead->forknum, blockddlrec->mainData);
            break;
        case BLOCK_DDL_TRUNCATE_RELNODE:
            xlog_block_smgr_redo_truncate(rnode, blockhead->blkno, blockhead->end_ptr);
            break;
        case BLOCK_DDL_DROP_RELNODE: {
            ColFileNodeRel *xnodes = (ColFileNodeRel *)blockddlrec->mainData;
            for (int i = 0; i < blockddlrec->rels; ++i) {
                ColFileNodeRel *colFileNodeRel = xnodes + i;
                ColFileNode colFileNode;
                ColFileNodeCopy(&colFileNode, colFileNodeRel);
                if (IS_COMPRESS_DELETE_FORK(colFileNode.forknum)) {
                    SET_OPT_BY_NEGATIVE_FORK(colFileNode.filenode, colFileNode.forknum);
                    colFileNode.forknum = MAIN_FORKNUM;
                }
                if (!IsValidColForkNum(colFileNode.forknum)) {
                    XlogDropRowReation(colFileNode.filenode);
                }
            }
            break;
        }
        default:
            break;
    }
}

void XLogBlockBcmCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
}

void XLogBlockNewCuCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
}

void XLogBlockClogCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
}

void XLogBlockCsnLogCommonRedo(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo)
{
}

static const XLogBlockRedoExtreRto g_xlogExtRtoRedoTable[BLOCK_DATA_CSNLOG_TYPE + 1] = {
    { XLogBlockDataCommonRedo, BLOCK_DATA_MAIN_DATA_TYPE }, { XLogBlockVmCommonRedo, BLOCK_DATA_VM_TYPE },
    { XLogBlockUndoCommonRedo, BLOCK_DATA_UNDO_TYPE },
    { XLogBlockFsmCommonRedo, BLOCK_DATA_FSM_TYPE },        { XLogBlockDdlCommonRedo, BLOCK_DATA_DDL_TYPE },
    { XLogBlockBcmCommonRedo, BLOCK_DATA_BCM_TYPE },        { XLogBlockNewCuCommonRedo, BLOCK_DATA_NEWCU_TYPE },
    { XLogBlockClogCommonRedo, BLOCK_DATA_CLOG_TYPE },      { XLogBlockCsnLogCommonRedo, BLOCK_DATA_CSNLOG_TYPE },
};

static inline bool IsHeap2Clean(const XLogBlockHead *blockhead)
{
    uint8 info = blockhead->xl_info & (~XLR_INFO_MASK);
    RmgrId rmid = blockhead->xl_rmid;

    return (rmid == RM_HEAP2_ID) && (info == XLOG_HEAP2_CLEAN);
}

static inline bool IsBtreeVacuum(const XLogBlockHead *blockhead)
{
    uint8 info = blockhead->xl_info & (~XLR_INFO_MASK);
    RmgrId rmid = blockhead->xl_rmid;

    return (rmid == RM_BTREE_ID) && (info == XLOG_BTREE_VACUUM);
}

static inline bool GetCleanupLock(const XLogBlockHead *blockhead)
{
    if (IsHeap2Clean(blockhead) || IsBtreeVacuum(blockhead)) {
        return true;
    }

    return false;
}

XLogRedoAction XLogBlockGetOperatorBuffer(XLogBlockHead *blockhead, void *blockrecbody, RedoBufferInfo *bufferinfo,
                                          bool notfound, ReadBufferMethod readmethod)
{
    uint16 block_valid = XLogBlockHeadGetValidInfo(blockhead);
    XLogRecPtr xlogLsn = XLogBlockHeadGetLSN(blockhead);
    XLogRedoAction redoaction = BLK_NOTFOUND;
    ReadBufferMode mode = RBM_NORMAL;

    if (bufferinfo->pageinfo.page != NULL) {
        return BLK_NEEDS_REDO;
    }

    XLogBlockInitRedoBlockInfo(blockhead, &bufferinfo->blockinfo);

    if (block_valid == BLOCK_DATA_MAIN_DATA_TYPE) {
        XLogBlockDataParse *blockdatarec = (XLogBlockDataParse *)blockrecbody;

        bool willinit = XLogBlockDataGetBlockFlags(blockdatarec) & BKPBLOCK_WILL_INIT;
        if ((willinit == false) && (notfound == true)) {
            return BLK_NOTFOUND;
        }

        bool getCleanupLock = GetCleanupLock(blockhead);
        if (willinit) {
            mode = RBM_ZERO_AND_LOCK;
        } else if (blockdatarec->blockhead.has_image) {
            if (getCleanupLock) {
                mode = RBM_ZERO_AND_CLEANUP_LOCK;
            } else {
                mode = RBM_ZERO_AND_LOCK;
            }
        } else if (blockhead->forknum > MAIN_FORKNUM) {
            mode = RBM_ZERO_ON_ERROR;
        }

        redoaction = XLogReadBufferForRedoBlockExtend(&bufferinfo->blockinfo, mode, getCleanupLock, bufferinfo, xlogLsn,
                                                      XLogBlockDataGetLastBlockLSN(blockdatarec), willinit, readmethod);
    } else if (block_valid == BLOCK_DATA_VM_TYPE || block_valid == BLOCK_DATA_FSM_TYPE) {
        redoaction = XLogReadBufferForRedoBlockExtend(&bufferinfo->blockinfo, RBM_ZERO_ON_ERROR, false, bufferinfo,
                                                      xlogLsn, true, InvalidXLogRecPtr, readmethod);
    } else if (block_valid == BLOCK_DATA_DDL_TYPE || block_valid == BLOCK_DATA_UNDO_TYPE) {
        redoaction = NO_BLK;
    }

    return redoaction;
}

void UpdateFsm(RedoBufferTag *blockInfo, Size freespace)
{
    uint16 slot;
    FSMAddress addr = fsm_get_location(blockInfo->blkno, &slot);
    BlockNumber blkno = fsm_logical_to_physical(addr);

    RedoBufferInfo fsmBufInfo = {0};
    fsmBufInfo.blockinfo.rnode = blockInfo->rnode;
    fsmBufInfo.blockinfo.forknum = FSM_FORKNUM;
    fsmBufInfo.blockinfo.blkno = blkno;
    fsmBufInfo.blockinfo.pblk.relNode = InvalidOid;
    XLogReadBufferForRedoBlockExtend(&fsmBufInfo.blockinfo, RBM_ZERO_ON_ERROR, false, &fsmBufInfo, InvalidXLogRecPtr,
                                     InvalidXLogRecPtr, true, G_BUFFERREADMETHOD);

    if (BufferIsValid(fsmBufInfo.buf)) {
        if (PageIsNew(fsmBufInfo.pageinfo.page))
            PageInit(fsmBufInfo.pageinfo.page, BLCKSZ, 0);

        int newCat = fsm_space_avail_to_cat(freespace);
        if (fsm_set_avail(fsmBufInfo.pageinfo.page, slot, newCat)) {
            if (ParseStateWithoutCache()) {
                SyncOneBufferForExtremRto(&fsmBufInfo);
            } else {
                MarkBufferDirtyHint(fsmBufInfo.buf, false);
            }
        }

        if (!ParseStateWithoutCache()) {
            UnlockReleaseBuffer(fsmBufInfo.buf);
        }
    }
}

void ExtremeRtoFlushBuffer(RedoBufferInfo *bufferinfo, bool updateFsm)
{
    Size freespace = 0;

   /*
    * FSM can not be read by physical location in recovery. It is possible to write on wrong places
    * if the FSM fork is dropped and then allocated when replaying old xlog.
    * Since FSM does not have to be totally accurate anyway, just skip it.
    */
    updateFsm = updateFsm && !IsSegmentFileNode(bufferinfo->blockinfo.rnode);

    if (updateFsm) {
        freespace = PageGetHeapFreeSpace(bufferinfo->pageinfo.page);
    }
    if (ParseStateWithoutCache()) {
        /* flush the block */
        if ((bufferinfo->pageinfo.page != NULL) && bufferinfo->dirtyflag) {
            SyncOneBufferForExtremRto(bufferinfo);
        }
        /* release buffer */
        XLogRedoBufferReleaseFunc(bufferinfo->buf);
    } else {
        if (bufferinfo->pageinfo.page != NULL) {
            BufferDesc *bufDesc = GetBufferDescriptor(bufferinfo->buf - 1);
            if (bufferinfo->dirtyflag || XLByteLT(bufDesc->lsn_on_disk, PageGetLSN(bufferinfo->pageinfo.page))) {
                MarkBufferDirty(bufferinfo->buf);
                if (!bufferinfo->dirtyflag && bufferinfo->blockinfo.forknum == MAIN_FORKNUM) {
                    int mode = WARNING;
#ifdef USE_ASSERT_CHECKING
                    mode = PANIC;
#endif
                    const uint32 shiftSz = 32;
                    ereport(mode, (errmsg("extreme_rto not mark dirty:lsn %X/%X, lsn_disk %X/%X, \
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

            UnlockReleaseBuffer(bufferinfo->buf); /* release buffer */
        }
    }

    if (updateFsm) {
        UpdateFsm(&bufferinfo->blockinfo, freespace);
    }
}

void XLogSynAllBuffer()
{
    if (G_BUFFERREADMETHOD == WITH_LOCAL_CACHE) {
        /* flush local buffer */
        LocalBufferFlushAllBuffer();
    }
}

bool XLogBlockRedoForExtremeRTO(XLogRecParseState *redoblocktate, RedoBufferInfo *bufferinfo,  bool notfound,
    RedoTimeCost &readBufCost, RedoTimeCost &redoCost)
{
    XLogRedoAction redoaction;
    uint16 block_valid;
    void *blockrecbody;
    XLogBlockHead *blockhead;
    long readcount = u_sess->instr_cxt.pg_buffer_usage->shared_blks_read;

    /* decode blockdata body */
    blockhead = &redoblocktate->blockparse.blockhead;
    blockrecbody = &redoblocktate->blockparse.extra_rec;
    block_valid = XLogBlockHeadGetValidInfo(blockhead);

    GetRedoStartTime(readBufCost);
    redoaction = XLogBlockGetOperatorBuffer(blockhead, blockrecbody, bufferinfo, notfound, G_BUFFERREADMETHOD);
    CountRedoTime(readBufCost);
    if (redoaction == BLK_NOTFOUND) {
#ifdef USE_ASSERT_CHECKING
        ereport(WARNING, (errmsg("XLogBlockRedoForExtremeRTO:lsn %X/%X, page %u/%u/%u %u not found",
                                 (uint32)(blockhead->end_ptr >> 32), (uint32)(blockhead->end_ptr), blockhead->spcNode,
                                 blockhead->dbNode, blockhead->relNode, blockhead->blkno)));
        DoRecordCheck(redoblocktate, InvalidXLogRecPtr, false);
#endif
        return true;
    }

    bool checkvalid = XLogBlockRefreshRedoBufferInfo(blockhead, bufferinfo);
    if (!checkvalid) {
        ereport(PANIC, (errmsg("XLogBlockRedoForExtremeRTO: redobuffer checkfailed")));
    }
    if (block_valid <= BLOCK_DATA_FSM_TYPE) {
        GetRedoStartTime(redoCost);
        Assert(block_valid == g_xlogExtRtoRedoTable[block_valid].block_valid);
        g_xlogExtRtoRedoTable[block_valid].xlog_redoextrto(blockhead, blockrecbody, bufferinfo);
        CountRedoTime(redoCost);
#ifdef USE_ASSERT_CHECKING
        if (block_valid != BLOCK_DATA_UNDO_TYPE && !bufferinfo->pageinfo.ignorecheck) {
            DoRecordCheck(redoblocktate, PageGetLSN(bufferinfo->pageinfo.page), true);
        }
#endif
        AddReadBlock(redoblocktate, (u_sess->instr_cxt.pg_buffer_usage->shared_blks_read - readcount));
    } else {
        ereport(WARNING, (errmsg("XLogBlockRedoForExtremeRTO: unsuport type %u, lsn %X/%X", (uint32)block_valid,
                                 (uint32)(blockhead->end_ptr >> 32), (uint32)(blockhead->end_ptr))));
    }
    return false;
}

static const XLogParseBlock g_xlogParseBlockTable[RM_MAX_ID + 1] = {
    { xlog_redo_parse_to_block, RM_XLOG_ID },
    { xact_redo_parse_to_block, RM_XACT_ID },
    { smgr_redo_parse_to_block, RM_SMGR_ID },
    { ClogRedoParseToBlock, RM_CLOG_ID },
    { DbaseRedoParseToBlock, RM_DBASE_ID },
    { tblspc_redo_parse_to_block, RM_TBLSPC_ID },
    { multixact_redo_parse_to_block, RM_MULTIXACT_ID },
    { relmap_redo_parse_to_block, RM_RELMAP_ID },
    { NULL, RM_STANDBY_ID },
    { Heap2RedoParseIoBlock, RM_HEAP2_ID },
    { HeapRedoParseToBlock, RM_HEAP_ID },
    { BtreeRedoParseToBlock, RM_BTREE_ID },
    { HashRedoParseToBlock, RM_HASH_ID },
    { GinRedoParseToBlock, RM_GIN_ID },
    { GistRedoParseToBlock, RM_GIST_ID },
    { seq_redo_parse_to_block, RM_SEQ_ID },
    { SpgRedoParseToBlock, RM_SPGIST_ID },
    { slot_redo_parse_to_block, RM_SLOT_ID },
    { Heap3RedoParseToBlock, RM_HEAP3_ID },
    { barrier_redo_parse_to_block, RM_BARRIER_ID },
#ifdef ENABLE_MOT
    { NULL, RM_MOT_ID },
#endif
    { UHeapRedoParseToBlock, RM_UHEAP_ID},
    { UHeap2RedoParseToBlock, RM_UHEAP2_ID},
    { UHeapUndoRedoParseToBlock, RM_UNDOLOG_ID},
    { UHeapUndoActionRedoParseToBlock, RM_UHEAPUNDO_ID},
    { UHeapRollbackFinishRedoParseToBlock, RM_UNDOACTION_ID},
    { UBTreeRedoParseToBlock, RM_UBTREE_ID },
    { UBTree2RedoParseToBlock, RM_UBTREE2_ID },
    { segpage_redo_parse_to_block, RM_SEGPAGE_ID }, 
    { NULL, RM_REPLORIGIN_ID },
};
inline XLogRecParseState *XLogParseToBlockCommonFunc(XLogReaderState *record, uint32 *blocknum)
{
    RmgrId rmid = XLogRecGetRmid(record);
    XLogRecParseState *blockparsestate = NULL;
    *blocknum = 0;

    if (rmid > RM_MAX_ID) {
        ereport(PANIC, (errmsg("XLogParseToBlockCommonFunc: rmid checkfailed")));
        return NULL;
    }
    Assert(rmid == g_xlogParseBlockTable[rmid].rmid);
    if (g_xlogParseBlockTable[rmid].xlog_parseblock != NULL)
        blockparsestate = g_xlogParseBlockTable[rmid].xlog_parseblock(record, blocknum);

    return blockparsestate;
}

XLogRecParseState *XLogParseToBlockForExtermeRTO(XLogReaderState *record, uint32 *blocknum)
{
    return XLogParseToBlockCommonFunc(record, blocknum);
}

void XLogBlockDispatchForExtermeRTO(XLogRecParseState *recordblockstate)
{
    XLogRecParseState *nextstate = recordblockstate;
    XLogBlockHead *blockhead = &nextstate->blockparse.blockhead;
    do {
        /* dispatch to batch redo thread */
        nextstate = (XLogRecParseState *)nextstate->nextrecord;
        blockhead = &nextstate->blockparse.blockhead;
    } while (nextstate != NULL);
}

#ifdef EXTREME_RTO_DEBUG_AB
void DoThreadExit()
{
    proc_exit(1);
}

void DoAllocFailed()
{
    palloc(0xFFFFFFFFFFFFFFFF);
}

void PanicFailed()
{
    ereport(PANIC, (errmsg("just do panic")));
}

void WaitLong()
{
    pg_usleep(1000000L);
}

AbnormalProcFunc g_AbFunList[ABNORMAL_NUM] = {
    DoThreadExit,
    DoAllocFailed,
    PanicFailed,
    WaitLong,
};
#endif
