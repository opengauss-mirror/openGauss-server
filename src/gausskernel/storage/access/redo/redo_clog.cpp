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
 * redo_clog.cpp
 *    parse clog xlog
 *
 * IDENTIFICATION
 *
 * src/gausskernel/storage/access/redo/redo_clog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "storage/smgr/fd.h"
#include "storage/proc.h"
#ifdef USE_ASSERT_CHECKING
#include "utils/builtins.h"
#endif /* USE_ASSERT_CHECKING */
#ifdef ENABLE_UT
#define static
#endif /* USE_UT */

XLogRecParseState *ClogXlogDdlParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    errno_t rc = EOK;
    int64 pageno = 0;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;
    int ddltype = BLOCK_DDL_TYPE_NONE;

    *blocknum = 0;
    if (info == CLOG_ZEROPAGE) {
        rc = memcpy_s(&pageno, sizeof(int64), XLogRecGetData(record), XLogRecGetDataLen(record));
        securec_check(rc, "", "");
        ddltype = BLOCK_DDL_CLOG_ZERO;

    } else if (info == CLOG_TRUNCATE) {
        rc = memcpy_s(&pageno, sizeof(int64), XLogRecGetData(record), XLogRecGetDataLen(record));
        securec_check(rc, "", "");
        ddltype = BLOCK_DDL_CLOG_TRUNCATE;
    } else {
        ereport(FATAL, (errmsg("ClogXlogDdlParseToBlock: unknown op code %u", info)));
    }

    forknum = (pageno >> LOW_BLOKNUMBER_BITS);
    lowblknum = (pageno & LOW_BLOKNUMBER_MASK);

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_DDL_TYPE, filenode, recordstatehead);

    XLogRecSetBlockDdlState(&(recordstatehead->blockparse.extra_rec.blockddlrec), ddltype,
                            (char *)XLogRecGetData(record), 1, compress);

    return recordstatehead;
}

XLogRecParseState *ClogRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;

    if ((info == CLOG_ZEROPAGE) || (info == CLOG_TRUNCATE)) {
        recordstatehead = ClogXlogDdlParseToBlock(record, blocknum);
    } else {
        ereport(PANIC, (errmsg("ClogXlogDdlParseToBlock: unknown op code %u", info)));
    }

    return recordstatehead;
}

XLogRecParseState *XactXlogClogParseToBlock(XLogReaderState *record, XLogRecParseState *recordstatehead,
                                            uint32 *blocknum, TransactionId xid, int nsubxids, TransactionId *subxids,
                                            CLogXidStatus status)
{
    uint64 pageno = TransactionIdToPage(xid); /* get page of parent */
    XLogRecParseState *blockstate = NULL;
    BlockNumber lowblknum;
    ForkNumber forknum;
    RelFileNodeForkNum filenode;
    uint16 offset[MAX_BLOCK_XID_NUMS];
    uint16 xidnum;
    TransactionId pagestartxid;

    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    forknum = (ForkNumber)(pageno >> LOW_BLOKNUMBER_BITS);
    lowblknum = (BlockNumber)(pageno & LOW_BLOKNUMBER_MASK);

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_CLOG_TYPE, filenode, blockstate);
    xidnum = 0;
    pagestartxid = CLogPageNoToStartXactId(pageno);

    offset[xidnum] = (xid - pagestartxid);
    xidnum++;
    for (int i = 0; i < nsubxids; i++) {
        if ((TransactionIdToPage(subxids[i]) != pageno) || (xidnum == MAX_BLOCK_XID_NUMS)) {
            XLogRecSetBlockCLogState(&blockstate->blockparse.extra_rec.blockclogrec, xid, status, xidnum, offset);
            /* clear */
            pageno = TransactionIdToPage(subxids[i]);
            xidnum = 0;
            pagestartxid = CLogPageNoToStartXactId(pageno);
            (*blocknum)++;
            XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }

            forknum = (ForkNumber)(pageno >> LOW_BLOKNUMBER_BITS);
            lowblknum = (BlockNumber)(pageno & LOW_BLOKNUMBER_MASK);
            filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
            XLogRecSetBlockCommonState(record, BLOCK_DATA_CLOG_TYPE, filenode, blockstate);
        }

        offset[xidnum] = (subxids[i] - pagestartxid);
        xidnum++;
    }

    XLogRecSetBlockCLogState(&blockstate->blockparse.extra_rec.blockclogrec, xid, status, xidnum, offset);
    return recordstatehead;
}
