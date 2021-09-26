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
 * slotfuncs.cpp
 *    parse repcalication slot xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/slotfuncs.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/decode.h"
#include "access/transam.h"
#include "utils/builtins.h"
#include "access/xlog_internal.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/pg_lsn.h"
#include "access/xlog.h"
#include "access/xlogproc.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "replication/replicainternal.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "storage/smgr/fd.h"

void XLogRecSetSlotState(XLogBlockSlotParse *blockslotstate, char *maindata, Size maindatalen)
{
    blockslotstate->maindatalen = maindatalen;
    blockslotstate->maindata = maindata;
}

XLogRecParseState *slot_xlog_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_SLOT_TYPE, filenode, recordstatehead);

    char *maindata = XLogRecGetData(record);
    Size maindatalen = XLogRecGetDataLen(record);
    XLogRecSetSlotState(&(recordstatehead->blockparse.extra_rec.blockslot), maindata, maindatalen);

    return recordstatehead;
}

XLogRecParseState *slot_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    switch (info) {
        case XLOG_SLOT_CREATE:
        case XLOG_SLOT_ADVANCE:
        case XLOG_SLOT_DROP:
        case XLOG_SLOT_CHECK:
            recordstatehead = slot_xlog_parse_to_block(record, blocknum);
            break;
        default:
            break;
    }
    return recordstatehead;
}
