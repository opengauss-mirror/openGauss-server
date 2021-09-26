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
 * xlog.cpp
 *    parse xlog xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/xlog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/xlogdefs.h"
#include "access/xlogproc.h"
#include "access/redo_common.h"
#include "storage/smgr/fd.h"

typedef enum {
    XLOG_FULL_PAGE_ORIG_BLOCK_NUM = 0,
} XLogFullPageWriteBlockEnum;

XLogRecParseState *xlog_fpi_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, XLOG_FULL_PAGE_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

void XLogRecSetXlogCommonState(XLogBlockXLogComParse *blockxlogstate, char *maindata, Size maindatalen,
                               XLogRecPtr readptr)
{
    blockxlogstate->readrecptr = readptr;
    blockxlogstate->maindatalen = maindatalen;
    blockxlogstate->maindata = maindata;
}

XLogRecParseState *xlog_xlog_common_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_XLOG_COMMON_TYPE, filenode, recordstatehead);
    char *maindata = XLogRecGetData(record);
    Size maindatalen = XLogRecGetDataLen(record);
    XLogRecSetXlogCommonState(&recordstatehead->blockparse.extra_rec.blockxlogcommon, maindata, maindatalen,
                              record->ReadRecPtr);
    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

XLogRecParseState *xlog_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;
    *blocknum = 0;

    /* in XLOG rmgr, backup blocks are only used by XLOG_FPI records */
    Assert(info == XLOG_FPI || info == XLOG_FPI_FOR_HINT || !XLogRecHasAnyBlockRefs(record));
    switch (info) {
        case XLOG_NEXTOID:
        case XLOG_CHECKPOINT_SHUTDOWN:
        case XLOG_CHECKPOINT_ONLINE:
        case XLOG_PARAMETER_CHANGE:
        case XLOG_FPW_CHANGE:
        case XLOG_BACKUP_END:
        case XLOG_DELAY_XLOG_RECYCLE:
            recordblockstate = xlog_xlog_common_parse_to_block(record, blocknum);
            break;
        case XLOG_FPI:
        case XLOG_FPI_FOR_HINT:
            recordblockstate = xlog_fpi_parse_to_block(record, blocknum);
            break;
        case XLOG_NOOP:
        case XLOG_SWITCH:
        case XLOG_RESTORE_POINT:
            break;
        default:
            ereport(PANIC, (errmsg("xlog_redo_parse_to_block: unknown op code %u", info)));
            break;
    }
    return recordblockstate;
}

void xlog_redo_data_block(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT) {
        XLogBlockDataParse *datadecode = blockdatarec;
        XLogRedoAction action;

        action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
        if (action != BLK_RESTORED) {
            const uint32 rightShiftSize = 32;
            ereport(ERROR, (errmsg("unexpected XLogReadBufferForRedo result when restoring backup %u/%u/%u "
                                   "forknumber %d block %u lsn %X/%X",
                                   blockhead->spcNode, blockhead->dbNode, blockhead->relNode, blockhead->forknum,
                                   blockhead->blkno, (uint32)(blockhead->end_ptr >> rightShiftSize),
                                   (uint32)blockhead->end_ptr)));
        }
    }
}
