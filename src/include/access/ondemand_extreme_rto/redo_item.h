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
 * redo_item.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/redo_item.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_REDO_ITEM_H
#define ONDEMAND_EXTREME_RTO_REDO_ITEM_H

#include "access/xlogreader.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/atomic.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"

#include "access/ondemand_extreme_rto/posix_semaphore.h"
#include "replication/replicainternal.h"

namespace ondemand_extreme_rto {

typedef struct RedoItem_s {
    bool needImmediateCheckpoint;
    bool needFullSyncCheckpoint;
    /* The expected timelines for this record. */
    List *expectedTLIs;
    /* The timestamp of the log record if it is a transaction record. */
    TimestampTz recordXTime;
    /* Next item on the free list. */
    struct RedoItem_s *freeNext;
    /* A "deep" copy of the log record. */
    XLogReaderState record;
    /* Used for really free */
    struct RedoItem_s *allocatedNext;
    TimestampTz syncXLogReceiptTime;
    int syncXLogReceiptSource;
    TransactionId RecentXmin;
    ServerMode syncServerMode;
} RedoItem;

static const int32 ANY_BLOCK_ID = -1;
static const uint32 ANY_WORKER = (uint32)-1;
static const uint32 TRXN_WORKER = (uint32)-2;
static const uint32 ALL_WORKER = (uint32)-3;
static const uint32 USTORE_WORKER = (uint32)-4;

void DumpItem(RedoItem *item, const char *funcName);

static inline RedoItem *GetRedoItemPtr(XLogReaderState *record)
{
    return (RedoItem *)(((char *)record) - offsetof(RedoItem, record));
}

}  // namespace ondemand_extreme_rto

#endif
