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
 * redo_csnlog.cpp
 *    parse csnlog xlog
 *
 * IDENTIFICATION
 *
 * src/gausskernel/storage/access/redo/redo_csnlog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xlogproc.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "utils/snapmgr.h"
#include "storage/barrier.h"
#include "storage/smgr/fd.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"

#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToCSNPage(xid) ((xid) / (TransactionId)CSNLOG_XACTS_PER_PAGE)
#define TransactionIdToCSNPgIndex(xid) ((xid) % (TransactionId)CSNLOG_XACTS_PER_PAGE)

#define MAX_CSNLOG_PAGE_NUMBER (TransactionIdToCSNPage(MaxTransactionId))

#define CSNLogPageNoToStartXactId(pageno) ((pageno > 0) ? ((pageno - 1) * CSNLOG_XACTS_PER_PAGE) : 0)

XLogRecParseState *XactXlogCsnlogParseToBlock(XLogReaderState *record, uint32 *blocknum, TransactionId xid,
                                              int nsubxids, TransactionId *subxids, CommitSeqNo csn,
                                              XLogRecParseState *recordstatehead)
{
    uint64 pageno = TransactionIdToCSNPage(xid); /* get page of parent */
    XLogRecParseState *blockstate = NULL;
    BlockNumber lowblknum;
    ForkNumber forknum;
    RelFileNodeForkNum filenode;
    uint16 offset[MAX_BLOCK_XID_NUMS];
    uint16 xidnum;
    TransactionId pagestartxid;

    if (csn == InvalidCommitSeqNo || xid == BootstrapTransactionId) {
        if (IsBootstrapProcessingMode())
            csn = COMMITSEQNO_FROZEN;
        else
            ereport(PANIC, (errmsg("cannot mark transaction %lu committed without CSN %lu", xid, csn)));
    }

    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }

    forknum = (ForkNumber)(pageno >> LOW_BLOKNUMBER_BITS);
    lowblknum = (BlockNumber)(pageno & LOW_BLOKNUMBER_MASK);

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_CSNLOG_TYPE, filenode, blockstate);
    xidnum = 0;
    pagestartxid = CSNLogPageNoToStartXactId(pageno);

    offset[xidnum] = (xid - pagestartxid);
    xidnum++;
    for (int i = 0; i < nsubxids; i++) {
        if ((TransactionIdToCSNPage(subxids[i]) != pageno) || (xidnum == MAX_BLOCK_XID_NUMS)) {
            XLogRecSetBlockCSNLogState(&blockstate->blockparse.extra_rec.blockcsnlogrec, xid, csn, xidnum, offset);
            /* clear */
            pageno = TransactionIdToCSNPage(subxids[i]);
            xidnum = 0;
            pagestartxid = CSNLogPageNoToStartXactId(pageno);

            (*blocknum)++;
            XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }

            forknum = (ForkNumber)(pageno >> LOW_BLOKNUMBER_BITS);
            lowblknum = (BlockNumber)(pageno & LOW_BLOKNUMBER_MASK);
            filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
            XLogRecSetBlockCommonState(record, BLOCK_DATA_CSNLOG_TYPE, filenode, blockstate);
        }

        offset[xidnum] = (subxids[i] - pagestartxid);
        xidnum++;
    }

    XLogRecSetBlockCSNLogState(&blockstate->blockparse.extra_rec.blockcsnlogrec, xid, csn, xidnum, offset);
    return recordstatehead;
}
