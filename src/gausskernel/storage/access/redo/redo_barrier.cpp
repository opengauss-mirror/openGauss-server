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
 * redo_barrier.cpp
 *    parse barrier xlog
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/access/redo/redo_barrier.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cbmparsexlog.h"
#include "access/gtm.h"
#include "access/xlog.h"
#include "access/xlogproc.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/barrier.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/fd.h"
#include "tcop/dest.h"
#include "securec_check.h"

void XLogRecSetBarrierState(XLogBlockBarrierParse *blockbarrierstate, char *mainData, Size len)
{
    blockbarrierstate->maindata = mainData;
    blockbarrierstate->maindatalen = len;
}

XLogRecParseState *barrier_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    RelFileNodeForkNum filenode;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    filenode = RelFileNodeForkNumFill(NULL, InvalidBackendId, forknum, lowblknum);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_BARRIER_TYPE, filenode, recordstatehead);

    XLogRecSetBarrierState(&(recordstatehead->blockparse.extra_rec.blockbarrier), XLogRecGetData(record),
                           XLogRecGetDataLen(record));
    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}
