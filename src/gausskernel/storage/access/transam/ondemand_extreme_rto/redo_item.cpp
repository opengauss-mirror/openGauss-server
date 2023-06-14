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
 * -------------------------------------------------------------------------
 *
 * redo_item.cpp
 *      Each RedoItem represents a log record ready to be replayed by one of
 *      the redo threads.  To decouple the lifetime of a RedoItem from its
 *      log record's original XLogReaderState, contents necessary for the
 *      actual replay are duplicated into RedoItem's internal XLogReaderState.

 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/ondemand_extreme_rto/redo_item.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <assert.h>
#include <string.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "utils/palloc.h"
#include "utils/guc.h"

#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/redo_item.h"
#include "postmaster/postmaster.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"

namespace ondemand_extreme_rto {
void DumpItem(RedoItem *item, const char *funcName)
{
    if (item == &g_redoEndMark || item == &g_terminateMark) {
        return;
    }
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]DiagLogRedoRecord: %s, ReadRecPtr:%lu,EndRecPtr:%lu,"
                            "imcheckpoint:%u, recordXTime:%lu,"
                            "syncXLogReceiptSource:%d, RecentXmin:%lu, syncServerMode:%u",
                            funcName, item->record.ReadRecPtr, item->record.EndRecPtr,
                            item->needImmediateCheckpoint, item->recordXTime,
                            item->syncXLogReceiptSource, item->RecentXmin, item->syncServerMode)));
    DiagLogRedoRecord(&(item->record), funcName);
}



}  // namespace ondemand_extreme_rto
