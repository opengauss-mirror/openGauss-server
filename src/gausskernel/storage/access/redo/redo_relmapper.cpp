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
 * relmapper.cpp
 *    parse relation map xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/relmapper.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogproc.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/lock/lwlock.h"
#include "utils/inval.h"
#include "utils/relmapper.h"

void XLogRecSetRelMapState(XLogBlockRelMapParse *blockrelmapstate, char *maindata, Size maindatalen)
{
    blockrelmapstate->maindatalen = maindatalen;
    blockrelmapstate->maindata = maindata;
}

XLogRecParseState *relmap_xlog_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_RELMAP_TYPE, filenode, recordstatehead);

    char *maindata = XLogRecGetData(record);
    Size maindatalen = XLogRecGetDataLen(record);
    XLogRecSetRelMapState(&(recordstatehead->blockparse.extra_rec.blockrelmap), maindata, maindatalen);

    return recordstatehead;
}

XLogRecParseState *relmap_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_RELMAP_UPDATE) {
        recordstatehead = relmap_xlog_parse_to_block(record, blocknum);
    } else {
        ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("relmap_redo_parse_to_block: unknown op code %u", info)));
    }
    return recordstatehead;
}
