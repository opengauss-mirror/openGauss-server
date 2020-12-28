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
#include "tcop/dest.h"
#include "securec_check.h"

void XLogRecSetBarrierState(XLogBlockBarrierParse *blockbarrierstate, XLogRecPtr startptr, XLogRecPtr endptr)
{
    blockbarrierstate->startptr = startptr;
    blockbarrierstate->endptr = endptr;
}

XLogRecParseState *barrier_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber lowblknum = InvalidBlockNumber;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockCommonState(record, BLOCK_DATA_BARRIER_TYPE, forknum, lowblknum, NULL, recordstatehead);

    XLogRecSetBarrierState(&(recordstatehead->blockparse.extra_rec.blockbarrier), record->ReadRecPtr,
                           record->EndRecPtr);
    return recordstatehead;
}
