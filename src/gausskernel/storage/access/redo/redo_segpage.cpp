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
                            BLOCK_DDL_TRUNCATE_RELNODE, false, NULL);

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

XLogRecParseState *segpage_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    if (info == XLOG_SEG_TRUNCATE) {
        recordstatehead = segpage_redo_parse_seg_truncate_to_block(record, blocknum);
    } else if (info == XLOG_SEG_SPACE_DROP) {
        recordstatehead = segpage_redo_parse_space_drop(record, blocknum);
    } else if (info == XLOG_SEG_SPACE_SHRINK) {
        recordstatehead = segpage_redo_parse_space_shrink(record, blocknum);
    } else {
        ereport(PANIC, (errmsg("segpage_redo_parse_to_block: unknown op code %u", info)));
    }
    return recordstatehead;
}
