/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * fsm_redo.cpp
 *
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/fsm/fsm_redo.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlogutils.h"
#include "access/fsm_redo.h"
#include "access/xlogproc.h"
#include "storage/freespace.h"
#include "storage/fsm_internals.h"
#include "storage/smgr.h"

#ifdef SAL_DFV_STORE
static void DoFsmXlogUpdate(XLogReaderState* record)
{
    FsmUpdateInfo* xlrec = (FsmUpdateInfo*)XLogRecGetData(record);
    if (xlrec->valueNum > NodesPerPage)
        ereport(PANIC,
            (errmsg("DoFsmXlogUpdate: valueNum(%u) is bigger than page max num(%lu)", xlrec->valueNum, NodesPerPage)));

    Buffer buffer;
    XLogRedoAction action = XLogReadFsmBufferForRedo(record, 0, &buffer);

    if (action == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buffer);

        if (PageIsNew(page)) {
            PageInit(BufferGetPage(buffer), BLCKSZ, 0);
        }

        FSMPage fsmpage = (FSMPage)PageGetContents(page);

        for (uint32 i = 0; i < xlrec->valueNum; ++i) {
            fsmpage->fp_nodes[xlrec->fsmInfo[i].nodeNo] = xlrec->fsmInfo[i].value;
        }
        MarkBufferDirty(buffer);
    }
    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}

void FsmXlogRedo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_FSM_UPDATE: {
            break;
        }
        case XLOG_FSM_FULL_PAGE_WRITE: {
            break;
        }
        case XLOG_FSM_TRUNCATE: {
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_LOG), errmsg("unkown type of fsm redo %u", info)));
    }
}
#endif
