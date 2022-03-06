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
 *        src/include/access/parallel_recovery/redo_item.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_RECOVERY_REDO_ITEM_H
#define PARALLEL_RECOVERY_REDO_ITEM_H

#include "access/xlogreader.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/atomic.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"

#include "access/parallel_recovery/posix_semaphore.h"
#include "replication/replicainternal.h"


namespace parallel_recovery {


typedef struct BTreeIncompleteAction {
    RelFileNode node;
    BlockNumber block;
    bool is_root;
} BTreeIncompleteAction;

typedef struct XLogInvalidPage {
    RelFileNode node;
    ForkNumber fork;
    BlockNumber minBlock;
    Oid dbid;
} XLogInvalidPage;

typedef union ContextUpdateData {
    BTreeIncompleteAction btree;
    XLogInvalidPage xlog;
} ContextUpdateData;

typedef enum ContextUpdateType {
    BTREE_FORGET_SPLIT,
    BTREE_FORGET_DELETE,
    XLOG_FORGET_PAGES,
    XLOG_FORGET_PAGES_DB
} ContextUpdateType;

typedef void (*ContextUpdateFunc)(const ContextUpdateData* data);

/*
 * Btree's incomplete_actions and XLog's invalid_page_tab need special
 * treatment.  Both use log_xxx() and forget_xxx() functions to maintain
 * inconsistent states during log replay.  Each of the log_xxx() functions
 * remembers an inconsistent condition, and each of the forget_xxx()
 * functions removes some inconsistent conditions.  Because the two
 * variables are thread local, the key to make it work under parallel log
 * replay is to make sure the log_xxx() and the forget_xxx() for matching
 * conditions are called in the same thread so they can correctly cancel
 * each other.  A ContextUpdateOp can be used to send a request to call
 * log_xxx() or forget_xxx() from one worker to another worker.  The
 * protocol is to set up the ContextUpdateOp during the redo function, and
 * after the record has been replayed each of the other workers checks
 * the ContextUpdateOp and calls the ContextUpdateFunc if it is the
 * ContextUpdateOp's designatedWorker.  See comments on incomplete_actions
 * and invalid_page_tab for detailed descriptions.
 */
typedef struct ContextUpdateOp {
    /* The type of the operation. */
    ContextUpdateType type;
    /* The data for the operation. */
    ContextUpdateData data;
    /* The function that performs the operation. */
    ContextUpdateFunc func;
    /* The id of the worker who needs to perform the operation. */
    uint32 designatedWorker;
    /* If the operation is valid. */
    bool inUse;
} ContextUpdateOp;

typedef struct RedoItem {
    bool sharewithtrxn; /* if ture when designatedWorker is trxn or all and need sync with pageworker */
    bool blockbytrxn;   /* if ture when designatedWorker is pagerworker and need sync with trxn */
    bool imcheckpoint;
    bool replay_undo;  /* if ture replay undo, otherwise replay data page */
    bool need_free;     /* bad block repair finish, need call pfree to free memory */
    /* Number of workers sharing this item. */
    uint32 shareCount;
    /* Number of page workers holding reference to this item + undo redo worker */
    pg_atomic_uint32 trueRefCount;
    /* Id of the worker designated to apply this item. */
    uint32 designatedWorker;
    /* The expected timelines for this record. */
    List* expectedTLIs;
    /* The timestamp of the log record if it is a transaction record. */
    TimestampTz recordXTime;
    /* Next item on each worker's list. */
    RedoItem** nextByWorker;
    /* Next item on the free list. */
    RedoItem* freeNext;
    /* Operation to update thread-local variables after replay. */
    ContextUpdateOp contextUpdateOp;
    /* Number of workers holding a reference to this item. */
    pg_atomic_uint32 refCount;
    /* If this item has been replayed. */
    pg_atomic_uint32 replayed;
    /* A "deep" copy of the log record. */
    XLogReaderState record;
    /* Used for really free */
    RedoItem* allocatedNext;
    TimestampTz syncXLogReceiptTime;
    int syncXLogReceiptSource;
    TransactionId RecentXmin;
    ServerMode syncServerMode;
    pg_atomic_uint32 freed;
    RedoItem *remoteNext;
} RedoItem;

static const int32 ANY_BLOCK_ID = -1;
static const uint32 ANY_WORKER = (uint32)-1;
static const uint32 TRXN_WORKER = (uint32)-2;
static const uint32 ALL_WORKER = (uint32)-3;
static const uint32 USTORE_WORKER = (uint32)-4;

RedoItem* CreateRedoItem(XLogReaderState* record, uint32 shareCount, uint32 designatedWorker, List* expectedTLIs,
    TimestampTz recordXTime, bool buseoriginal);
RedoItem* CreateLSNMarker(XLogReaderState* record, List* expectedTLIs, bool buseoriginal = false);

bool IsLSNMarker(const RedoItem* item);

static inline RedoItem* GetRedoItemPtr(XLogReaderState* record)
{
    return (RedoItem*)(((char*)record) - offsetof(RedoItem, record));
}


}

#endif
