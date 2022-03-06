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
 * storage.cpp
 *    parse storage xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/storage.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogproc.h"
#include "access/redo_common.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr/fd.h"

XLogRecParseState *smgr_xlog_relnode_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;
    RelFileNodeOld *rnode = NULL;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber blkno = InvalidBlockNumber;
    int ddltype;
    bool colmrel = false;

    if (info == XLOG_SMGR_CREATE) {
        xl_smgr_create *xlrec = (xl_smgr_create *)XLogRecGetData(record);
        rnode = &(xlrec->rnode);
        forknum = xlrec->forkNum;
        ddltype = BLOCK_DDL_CREATE_RELNODE;
        colmrel = IsValidColForkNum(xlrec->forkNum);
    } else {
        xl_smgr_truncate *xlrec = (xl_smgr_truncate *)XLogRecGetData(record);
        rnode = &(xlrec->rnode);
        blkno = xlrec->blkno;
        ddltype = BLOCK_DDL_TRUNCATE_RELNODE;
    }

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, *rnode, XLogRecGetBucketId(record));

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&tmp_node, InvalidBackendId, forknum, blkno);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_DDL_TYPE, filenode, recordstatehead);

    XLogRecSetBlockDdlState(&(recordstatehead->blockparse.extra_rec.blockddlrec), ddltype,
                            (char *)XLogRecGetData(record));
    return recordstatehead;
}

XLogRecParseState *smgr_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    if ((info == XLOG_SMGR_CREATE) || (info == XLOG_SMGR_TRUNCATE)) {
        recordstatehead = smgr_xlog_relnode_parse_to_block(record, blocknum);
    } else {
        ereport(PANIC, (errmsg("smgr_redo_parse_to_block: unknown op code %u", info)));
    }
    return recordstatehead;
}

