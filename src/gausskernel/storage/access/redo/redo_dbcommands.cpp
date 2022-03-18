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
 * redo_dbcommands.cpp
 *    parse ddl xlog
 *
 * IDENTIFICATION
 *
 * src/gausskernel/storage/access/redo/redo_dbcommands.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogproc.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "storage/smgr/fd.h"

void XLogRecSetBlockDataBaseState(XLogBlockDataBaseParse *blockdatastate, Oid srcdbid, Oid srctbsid)
{
    blockdatastate->src_db_id = srcdbid;
    blockdatastate->src_tablespace_id = srctbsid;
}

XLogRecParseState *DatabaseXlogCommonParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;
    RelFileNode rnode;
    RelFileNodeForkNum filenode;
    XLogBlockParseEnum blockparsetype;
    Oid srcdatabaseid = InvalidOid;
    Oid srctablespaceid = InvalidOid;

    if (info == XLOG_DBASE_CREATE) {
        xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *)XLogRecGetData(record);

        rnode.dbNode = xlrec->db_id;
        rnode.spcNode = xlrec->tablespace_id;
        rnode.relNode = InvalidOid;
        srcdatabaseid = xlrec->src_db_id;
        srctablespaceid = xlrec->src_tablespace_id;
        blockparsetype = BLOCK_DATA_CREATE_DATABASE_TYPE;
    } else {
        xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *)XLogRecGetData(record);
        rnode.dbNode = xlrec->db_id;
        rnode.spcNode = xlrec->tablespace_id;
        rnode.relNode = InvalidOid;
        blockparsetype = BLOCK_DATA_DROP_DATABASE_TYPE;
    }

    rnode.bucketNode = InvalidBktId;
    rnode.opt = 0;
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, blockparsetype, filenode, recordstatehead);

    XLogRecSetBlockDataBaseState(&(recordstatehead->blockparse.extra_rec.blockdatabase), srcdatabaseid,
                                 srctablespaceid);
    return recordstatehead;
}

XLogRecParseState *DbaseRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;

    if ((info == XLOG_DBASE_CREATE) || (info == XLOG_DBASE_DROP)) {
        recordstatehead = DatabaseXlogCommonParseToBlock(record, blocknum);
    } else {
        ereport(PANIC,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("DbaseRedoParseToBlock: unknown op code %u", info)));
    }
    return recordstatehead;
}
