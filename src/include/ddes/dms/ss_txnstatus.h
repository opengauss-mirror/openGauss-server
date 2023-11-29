/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_txnstatus.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_txnstatus.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SS_TXNSTATUS_H
#define SS_TXNSTATUS_H

#include "postgres.h"
#include "c.h"
#include "access/clog.h"

typedef struct TxnStatusEntry {
    TransactionId xid;
    CommitSeqNo csn;
    CLogXidStatus clogStatus;
    TxnStatusEntry* prev;
    TxnStatusEntry* next;
    volatile uint32 queueId;
    uint32 refcnt;
} TxnStatusEntry;

typedef struct LRUQueue {
    unsigned int id;
    unsigned int usedCount;
    unsigned int maxSize;
    TxnStatusEntry *head;
    TxnStatusEntry *tail;
} LRUQueue;

#define TXNSTATUS_CACHE_LRU_FFACTOR 0.9
#define TXNSTATUS_CACHE_INVALID_QUEUEID (NUM_TXNSTATUS_CACHE_PARTITIONS + 1)
#define NUM_TXNSTATUS_CACHE_ENTRIES (g_instance.attr.attr_storage.dms_attr.txnstatus_cache_size)
#define TxnStatusCachePartitionId(hashcode) ((hashcode) % NUM_TXNSTATUS_CACHE_PARTITIONS)
#define TxnStatusCachePartitionLock(hashcode) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstTxnStatusCacheLock + TxnStatusCachePartitionId(hashcode)].lock)
#define TXNSTATUS_LWLOCK_ACQUIRE(hashcode, lockmode) \
    ((void)LWLockAcquire(TxnStatusCachePartitionLock(hashcode), lockmode))
#define TXNSTATUS_LWLOCK_RELEASE(hashcode) (LWLockRelease(TxnStatusCachePartitionLock(hashcode)))

uint32 XidHashCode(const TransactionId *xid);
void SSInitTxnStatusCache();
int TxnStatusCacheInsert(const TransactionId *xid, uint32 hashcode, CommitSeqNo csn, CLogXidStatus clogStatus);
bool TxnStatusCacheLookup(TransactionId *xid, uint32 hashcode, CommitSeqNo *cached);
int TxnStatusCacheDelete(TransactionId *xid, uint32 hashcode);

#endif /* SS_TXNSTATUS_H */