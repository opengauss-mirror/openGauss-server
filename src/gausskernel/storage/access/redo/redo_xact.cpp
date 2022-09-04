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
 * xact.cpp
 *    parse xact xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/xact.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "access/clog.h"
#include "access/csnlog.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "access/multi_redo_api.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "commands/sequence.h"
#include "executor/spi.h"
#include "libpq/be-fsstubs.h"
#include "pgxc/groupmgr.h"
#include "replication/datasyncrep.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "storage/smgr/fd.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "utils/combocid.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/plog.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "access/redo_common.h"

/*
 * *************Add for batchredo begin***************
 * for these func ,they don't hold the lock and parse the record
 * they only operator the page, and update page's LSN
 * they are shared by Normal redo、ExtremRTO and DFV
 * **************************************************
 */
XLogRecParseState *xact_redo_rmddl_parse_to_block(XLogReaderState *record, XLogRecParseState *recordstatehead,
                                                  uint32 *blocknum, ColFileNodeRel *xnodes, int nrels)
{
    XLogRecParseState *blockstate = NULL;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    Assert(nrels > 0);
    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, MAIN_FORKNUM, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_DDL_TYPE, filenode, blockstate);
    XLogRecSetBlockDdlState(&(blockstate->blockparse.extra_rec.blockddlrec), BLOCK_DDL_DROP_RELNODE, (char *)xnodes,
        nrels, compress);

    return recordstatehead;
}

static XLogRecParseState *xact_xlog_commit_internal_parse(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    TransactionId xid = XLogRecGetXid(record);
    CommitSeqNo commitseql = COMMITSEQNO_INPROGRESS;
    TransactionId *sub_xids = NULL;
    int nsubxacts;
    uint64 csn;
    TransactionId max_xid;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;

    if (info != XLOG_XACT_COMMIT_COMPACT) {
        xl_xact_commit *xlrec = NULL;
        if (info == XLOG_XACT_COMMIT) {
            xlrec = (xl_xact_commit *)XLogRecGetData(record);
        } else {
            xl_xact_commit_prepared *xlrecpre = (xl_xact_commit_prepared *)XLogRecGetData(record);
            xlrec = &(xlrecpre->crec);
            xid = xlrecpre->xid; /* prepare for special */
        }

        sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
        nsubxacts = xlrec->nsubxacts;
        csn = xlrec->csn;
    } else {
        xl_xact_commit_compact *xlrec = (xl_xact_commit_compact *)XLogRecGetData(record);

        sub_xids = xlrec->subxacts;
        nsubxacts = xlrec->nsubxacts;

        csn = xlrec->csn;
    }

    max_xid = TransactionIdLatest(xid, nsubxacts, sub_xids);

    recordstatehead = XactXlogClogParseToBlock(record, recordstatehead, blocknum, xid, nsubxacts, sub_xids,
                                               CLOG_XID_STATUS_COMMITTED);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (csn >= COMMITSEQNO_FROZEN) {
        commitseql = csn;
    } else if (csn == COMMITSEQNO_INPROGRESS) {
        commitseql = COMMITSEQNO_FROZEN;
    }
    recordstatehead = XactXlogCsnlogParseToBlock(record, blocknum, xid, nsubxacts, sub_xids, commitseql,
                                                 recordstatehead);
    if (recordstatehead == NULL) {
        return NULL;
    }

    recordstatehead = xact_xlog_commit_parse_to_block(record, recordstatehead, blocknum, max_xid, commitseql);

    return recordstatehead;
}

static XLogRecParseState *xact_xlog_abort_internal_parse(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    TransactionId xid = XLogRecGetXid(record);
    TransactionId *sub_xids = NULL;
    int nsubxacts;
    TransactionId max_xid;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;

    xl_xact_abort *xlrec = NULL;
    if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_WITH_XID) {
        xlrec = (xl_xact_abort *)XLogRecGetData(record);
    } else {
        xl_xact_abort_prepared *xlrecpre = (xl_xact_abort_prepared *)XLogRecGetData(record);
        xlrec = &(xlrecpre->arec);
        xid = xlrecpre->xid; /* prepare for special */
    }

    sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
    nsubxacts = xlrec->nsubxacts;

    max_xid = TransactionIdLatest(xid, nsubxacts, sub_xids);

    recordstatehead = XactXlogClogParseToBlock(record, recordstatehead, blocknum, xid, nsubxacts, sub_xids,
                                               CLOG_XID_STATUS_ABORTED);
    if (recordstatehead == NULL) {
        return NULL;
    }

    recordstatehead = XactXlogCsnlogParseToBlock(record, blocknum, xid, nsubxacts, sub_xids, COMMITSEQNO_ABORTED,
                                                 recordstatehead);
    if (recordstatehead == NULL) {
        return NULL;
    }

    recordstatehead = xact_xlog_abort_parse_to_block(record, recordstatehead, blocknum, max_xid, COMMITSEQNO_ABORTED);

    return recordstatehead;
}

static XLogRecParseState *xact_xlog_prepare_internal_parse(XLogReaderState *record, uint32 *blocknum)
{
    TransactionId xid;
    TransactionId *sub_xids = NULL;
    int nsubxacts;
    TransactionId max_xid;
    XLogRecParseState *recordstatehead = NULL;

    char *recorddata = XLogRecGetData(record);
    TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *)recorddata;

    *blocknum = 0;
    if (TransactionIdIsNormal(hdr->xid)) {
        bool compressMagic = hdr->magic == TWOPHASE_MAGIC_COMPRESSION;
        int hdrSize = (hdr->magic == TWOPHASE_MAGIC_NEW || compressMagic) ?
            sizeof(TwoPhaseFileHeaderNew) : sizeof(TwoPhaseFileHeader);
        sub_xids = (TransactionId *)(recorddata + MAXALIGN(hdrSize));
        nsubxacts = hdr->nsubxacts;

        xid = hdr->xid;

        recordstatehead = XactXlogCsnlogParseToBlock(record, blocknum, xid, nsubxacts, sub_xids,
                                                     COMMITSEQNO_COMMIT_INPROGRESS, recordstatehead);
        if (recordstatehead == NULL) {
            return NULL;
        }
    }
    max_xid = XLogRecGetXid(record);
    recordstatehead = xact_xlog_prepare_parse_to_block(record, recordstatehead, blocknum, max_xid);

    return recordstatehead;
}

XLogRecParseState *xact_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    /* Backup blocks are not used in xact records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if ((info == XLOG_XACT_COMMIT_COMPACT) || (info == XLOG_XACT_COMMIT) || (info == XLOG_XACT_COMMIT_PREPARED)) {
        recordblockstate = xact_xlog_commit_internal_parse(record, blocknum);
    } else if ((info == XLOG_XACT_ABORT) || (info == XLOG_XACT_ABORT_PREPARED) || (info == XLOG_XACT_ABORT_WITH_XID)) {
        recordblockstate = xact_xlog_abort_internal_parse(record, blocknum);
    } else if (info == XLOG_XACT_PREPARE) {
        recordblockstate = xact_xlog_prepare_internal_parse(record, blocknum);
    } else if (info == XLOG_XACT_ASSIGNMENT) {
        const uint32 rightShiftSize = 32;
        ereport(WARNING, (errmsg("xact_redo_parse_to_block: XLOG_XACT_ASSIGNMENT log(%X/%X) could not be here!!",
                                 (uint32)(record->EndRecPtr >> rightShiftSize), (uint32)(record->EndRecPtr))));
    } else
        ereport(PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("xact_redo_parse_to_block: unknown op code %u", info)));

    return recordblockstate;
}

/*
 * *************Add for batchredo end****************
 * for these func ,they don't hold the lock and parse the record
 * they only operator the page, and update page's LSN
 * they are shared by Normal redo、ExtremRTO and DFV
 * **************************************************
 */
/*
 * ************Add for batchredo begin****************
 * for these func ,they don't hold the lock and parse the record
 * they only operator the page, and update page's LSN
 * they are shared by Normal redo、ExtremRTO and DFV
 * ***************************************************
 */
XLogRecParseState *xact_xlog_commit_parse_to_block(XLogReaderState *record, XLogRecParseState *recordstatehead,
                                                   uint32 *blocknum, TransactionId maxxid, CommitSeqNo maxseqnum)
{
    XLogRecParseState *blockstate = NULL;
    RelFileNode relnode;
    XLogRecPtr globalDelayDDLLSN;
    XLogRecPtr lsn = record->EndRecPtr;
    bool delayddlflag = false;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    uint64 xrecinfo = 0;
    TimestampTz xact_time;
    uint8 updateminrecovery = false;
    ColFileNodeRel *xnodes = NULL;
    int nrels = 0;
    SharedInvalidationMessage *inval_msgs = NULL;
    int invalidmsgnum = 0;
    int nlibrary = 0;
    char *libfilename = NULL;

    relnode.dbNode = InvalidOid;
    relnode.spcNode = InvalidOid;
    relnode.relNode = InvalidOid;
    relnode.bucketNode = InvalidBktId;
    relnode.opt = 0;

    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }

    if (info != XLOG_XACT_COMMIT_COMPACT) {
        xl_xact_commit *xlrec = NULL;
        TransactionId *subxacts = NULL;
        int nsubxacts = 0;
        if (info == XLOG_XACT_COMMIT) {
            xlrec = (xl_xact_commit *)XLogRecGetData(record);
        } else {
            xl_xact_commit_prepared *xlrecpre = (xl_xact_commit_prepared *)XLogRecGetData(record);
            xlrec = &(xlrecpre->crec);
        }

        xrecinfo = xlrec->xinfo;
        xact_time = xlrec->xact_time;
        xnodes = xlrec->xnodes;
        nrels = xlrec->nrels;

        if (nrels > 0) {
            updateminrecovery = true;
            globalDelayDDLLSN = GetDDLDelayStartPtr();
            if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, lsn))
                delayddlflag = true;
            else
                delayddlflag = false;
        }

        /* subxid array follows relfilenodes */
        subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
        nsubxacts = xlrec->nsubxacts;
        /* invalidation messages array follows subxids */
        inval_msgs = (SharedInvalidationMessage *)&(subxacts[xlrec->nsubxacts]);
        invalidmsgnum = xlrec->nmsgs;
        nlibrary = xlrec->nlibrary;

        if (nlibrary > 0) {
            libfilename = (char *)xnodes + (nrels * SIZE_OF_COLFILENODE(compress)) +
                          (nsubxacts * sizeof(TransactionId)) + (invalidmsgnum * sizeof(SharedInvalidationMessage));
        }

        relnode.dbNode = xlrec->dbId;
        relnode.spcNode = xlrec->tsId;
    } else {
        xl_xact_commit_compact *xlrec = (xl_xact_commit_compact *)XLogRecGetData(record);
        xact_time = xlrec->xact_time;
    }

    RelFileNodeForkNum filenode =
        RelFileNodeForkNumFill(&relnode, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_XACTDATA_TYPE, filenode, blockstate);

    XLogRecSetXactCommonState(&blockstate->blockparse.extra_rec.blockxact, info, xrecinfo, xact_time);

    XLogRecSetXactRecoveryState(&blockstate->blockparse.extra_rec.blockxact, maxxid, maxseqnum, delayddlflag,
                                updateminrecovery);
    XLogRecSetXactDdlState(&blockstate->blockparse.extra_rec.blockxact, nrels, (void *)xnodes, invalidmsgnum,
                           (void *)inval_msgs, nlibrary, (void *)libfilename);
    if (nrels > 0) {
        recordstatehead = xact_redo_rmddl_parse_to_block(record, recordstatehead, blocknum, xnodes, nrels);
    }
    return recordstatehead;
}

XLogRecParseState *xact_xlog_abort_parse_to_block(XLogReaderState *record, XLogRecParseState *recordstatehead,
                                                  uint32 *blocknum, TransactionId maxxid, CommitSeqNo maxseqnum)
{
    XLogRecParseState *blockstate = NULL;
    XLogRecPtr globalDelayDDLLSN;
    XLogRecPtr lsn = record->EndRecPtr;
    bool delayddlflag = false;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    char *libfilename = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }

    xl_xact_abort *xlrec = NULL;
    if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_WITH_XID) {
        xlrec = (xl_xact_abort *)XLogRecGetData(record);
    } else {
        xl_xact_abort_prepared *xlrecpre = (xl_xact_abort_prepared *)XLogRecGetData(record);
        xlrec = &(xlrecpre->arec);
    }
    TimestampTz xact_time = xlrec->xact_time;
    ColFileNodeRel *xnodes = xlrec->xnodes;
    int nrels = xlrec->nrels;
    int nsubxacts = xlrec->nsubxacts;

    if (nrels > 0) {
        globalDelayDDLLSN = GetDDLDelayStartPtr();
        if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, lsn))
            delayddlflag = true;
        else
            delayddlflag = false;
    }

    int nlibrary = xlrec->nlibrary;
    if (nlibrary > 0) {
        libfilename = (char *)xnodes + (nrels * SIZE_OF_COLFILENODE(compress)) + (nsubxacts * sizeof(TransactionId));
    }

    if (info == XLOG_XACT_ABORT_WITH_XID) {
        libfilename += sizeof(TransactionId);
    }

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_XACTDATA_TYPE, filenode, blockstate);

    XLogRecSetXactCommonState(&blockstate->blockparse.extra_rec.blockxact, info, 0, xact_time);

    XLogRecSetXactRecoveryState(&blockstate->blockparse.extra_rec.blockxact, maxxid, maxseqnum, delayddlflag, false);
    XLogRecSetXactDdlState(&blockstate->blockparse.extra_rec.blockxact, nrels, (void *)xnodes, 0, NULL, nlibrary,
                           (void *)libfilename);
    if (nrels > 0) {
        recordstatehead = xact_redo_rmddl_parse_to_block(record, recordstatehead, blocknum, xnodes, nrels);
    }
    return recordstatehead;
}

void XLogRecSetPreparState(XLogBlockPrepareParse *blockpreparestate, TransactionId maxxid, char *maindata,
                           Size maindatalen)
{
    blockpreparestate->maxxid = maxxid;
    blockpreparestate->maindata = maindata;
    blockpreparestate->maindatalen = maindatalen;
}

XLogRecParseState *xact_xlog_prepare_parse_to_block(XLogReaderState *record, XLogRecParseState *recordstatehead,
                                                    uint32 *blocknum, TransactionId maxxid)
{
    XLogRecParseState *blockstate = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListStateFunc(record, &blockstate, &recordstatehead);
    if (blockstate == NULL) {
        return NULL;
    }

    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_PREPARE_TYPE, filenode, blockstate);
    char *maindata = XLogRecGetData(record);
    Size maindatalen = XLogRecGetDataLen(record);
    XLogRecSetPreparState(&blockstate->blockparse.extra_rec.blockprepare, maxxid, maindata, maindatalen);
    return recordstatehead;
}

/*
 * ************Add for batchredo end****************
 * for these func ,they don't hold the lock and parse the record
 * they only operator the page, and update page's LSN
 * they are shared by Normal redo, ExtremRTO and DFV
 * *************************************************
 */
