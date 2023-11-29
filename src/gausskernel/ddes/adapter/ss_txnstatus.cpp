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
 * ss_txnstatus.cpp
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_txnstatus.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_txnstatus.h"
#include "postgres.h"
#include "utils/dynahash.h"
#include "access/transam.h"

static inline int LRUIsFull(LRUQueue* queue)
{
    return queue->usedCount >= queue->maxSize;
}

static inline int LRUIsEmpty(LRUQueue* queue)
{
    return queue->usedCount == 0;
}

static inline unsigned int LRUGetUsage(LRUQueue* queue)
{
    return queue->usedCount;
}

static inline unsigned int LRUGetMax(LRUQueue* queue)
{
    return queue->maxSize;
}

static inline TxnStatusEntry* getTailLRUEntry(LRUQueue* queue)
{
    return queue->tail;
}

static inline TxnStatusEntry* getHeadLRUEntry(LRUQueue* queue)
{
    return queue->head;
}

static inline void updateEvictionRefcnt(TxnStatusEntry* entry)
{
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_evictions++;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_eviction_refcnt += entry->refcnt;
    entry->refcnt = 0;
}

/*
 * Internal function; caller makes sure action is needed.
 * Caller clears evicted entry's TxnStatus values if needed.
 */
static TxnStatusEntry* dequeueLRUEntry(LRUQueue* queue)
{
    if (unlikely(LRUIsEmpty(queue))) {
        ereport(PANIC, (errmsg("SSTxnStatusCache LRU empty, used=%u, max=%u",
            LRUGetUsage(queue), LRUGetMax(queue))));
    }

    TxnStatusEntry* temp = queue->tail;
    if (temp->prev == NULL) {
        queue->head = queue->tail = NULL;
    } else {
        queue->tail = temp->prev;
        queue->tail->next = NULL;   
    }
    queue->usedCount--;

    temp->prev = NULL;
    temp->next = NULL;
    temp->queueId = TXNSTATUS_CACHE_INVALID_QUEUEID;
    updateEvictionRefcnt(temp);
    return temp;
}

/* 
 * Internal function, caller does eviction in advance if necessary.
 * Caller validates entry's TxnStatus values.
 */
static void enqueueLRUEntry(LRUQueue* queue, TxnStatusEntry* entry)
{
    if (unlikely(LRUIsFull(queue))) {
        ereport(PANIC, (errmsg("SSTxnStatusCache LRU is, usage=%u, max=%u",
            LRUGetUsage(queue), LRUGetMax(queue))));
    }

    entry->prev = NULL;
    entry->next = NULL;
    entry->refcnt = 0;
    if (LRUIsEmpty(queue)) {
        queue->head = queue->tail = entry;
    } else {
        queue->head->prev = entry;
        entry->next = queue->head;
        queue->head = entry;
    }

    entry->queueId = queue->id;
    queue->usedCount++;
}

/* caller makes sure referred entry is extant */
static void referLRUEntry(LRUQueue* queue, TxnStatusEntry* entry)
{
    entry->refcnt++;
    if (entry->prev == NULL) {
        return;
    }

    entry->prev->next = entry->next;
    if (entry->next == NULL) {
        queue->tail = entry->prev;
        queue->tail->next = NULL;
    } else {
        entry->next->prev = entry->prev;
    }

    entry->next = queue->head;
    entry->prev = NULL;
    entry->next->prev = entry;
    queue->head = entry;
}


/*
 * Compute the hash code associated with a Xid.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per procedure, and then pass it around as needed.  Aside from
 * passing the hashcode to hash_search_with_hash_value(), we can extract
 * the lock partition number from the hashcode.
 */
uint32 XidHashCode(const TransactionId *xid)
{
    return get_hash_value(t_thrd.dms_cxt.SSTxnStatusHash, (const void *)xid);
}

/*
 * Init TxnStatusLRU.
 */
static void initTxnStatusLRU(uint32 size)
{   
    bool lruInited = false;
    uint32 lruSize = size / NUM_TXNSTATUS_CACHE_PARTITIONS;
    ereport(DEBUG1, (errmodule(MOD_SS_TXNSTATUS),
        errmsg("SSTxnStatusCache totalsz=%u, partcnt=LRUcnt=%u, LRUsz=%um.",
            size, NUM_TXNSTATUS_CACHE_PARTITIONS, lruSize)));

    t_thrd.dms_cxt.SSTxnStatusLRU = (LRUQueue *)CACHELINEALIGN(ShmemInitStruct("SS Xid LRU cache",
        NUM_TXNSTATUS_CACHE_PARTITIONS * sizeof(LRUQueue) + PG_CACHE_LINE_SIZE, &lruInited));
    if (!lruInited) {
        for (unsigned int i = 0; i < NUM_TXNSTATUS_CACHE_PARTITIONS; i++) {
            LRUQueue *queue = &t_thrd.dms_cxt.SSTxnStatusLRU[i];
            queue->usedCount = 0;
            queue->maxSize = lruSize;
            queue->head = NULL;
            queue->tail = NULL;
            queue->id = i;
        }
    }
}

/*
 * Initialize shmem hash table for mapping txninfo
 */
static void InitTxnStatusTable(unsigned int maxsize)
{
    HASHCTL hctl;
    int hflags;
    long initSize;

    /*
     * Init max size to request for TxnStatus hashtables.
     */
    initSize = maxsize;

    /*
     * Allocate hash table for TxnStatusEntry structs.
     */
    errno_t rc = memset_s(&hctl, sizeof(hctl), 0, sizeof(hctl));
    securec_check(rc, "", "");
    hctl.keysize = sizeof(TransactionId);
    hctl.entrysize = sizeof(TxnStatusEntry);
    hctl.hash = tag_hash;
    hctl.num_partitions = NUM_TXNSTATUS_CACHE_PARTITIONS;
    hflags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
    t_thrd.dms_cxt.SSTxnStatusHash = ShmemInitHash("TxnStatus hash",
        initSize, maxsize, &hctl, hflags);
}

void SSInitTxnStatusCache()
{
    if (!ENABLE_DMS || !ENABLE_SS_TXNSTATUS_CACHE) {
        return;
    }

    Assert(NUM_TXNSTATUS_CACHE_ENTRIES % NUM_TXNSTATUS_CACHE_PARTITIONS == 0);
    initTxnStatusLRU(NUM_TXNSTATUS_CACHE_ENTRIES);
    InitTxnStatusTable(NUM_TXNSTATUS_CACHE_ENTRIES);
}

/*
 * TxnStatusCacheInsert
 * Look up the hashtable entry for given XID, which may not exist
 *
 * Caller does not need to hold partlock; this function decides locking S or X.
 */
int TxnStatusCacheInsert(const TransactionId *xid, uint32 hashcode, CommitSeqNo csn, CLogXidStatus clogStatus)
{
    uint32 evictHash;
    bool found = false;
    uint32 ret = GS_SUCCESS;
    TxnStatusEntry *insertEntry = NULL;
    TxnStatusEntry *evictEntry = NULL;
    HTAB* hashp = t_thrd.dms_cxt.SSTxnStatusHash;
    const uint32 queueId = TxnStatusCachePartitionId(hashcode);
    LRUQueue* queue = &t_thrd.dms_cxt.SSTxnStatusLRU[queueId];
    bool triggered = LRUGetUsage(queue) * 1.0 / LRUGetMax(queue) >= TXNSTATUS_CACHE_LRU_FFACTOR;

    /* 
     * Trigger LRU-based entry eviction on-demand: peek tail, rm hash, and then rm LRU.
     * Concurrency would occur were multiple xid insertions happen to evict the same xid,
     * subpartition of which is lockfree, resulting in either the 2nd eviction failure during
     * htable deletion, or both succeeds, causing the later evicts an extra LRU entry.
     * Eviction concurreny is no longer worried as LRU partlock now guarantees consistency.
     */
    if (triggered) {
        evictEntry = dequeueLRUEntry(queue);
        evictHash = XidHashCode(&evictEntry->xid);
        ereport(DEBUG1, (errmodule(MOD_SS_TXNSTATUS),
            errmsg("SSTxnStatusCache eviction xid=%lu, queue=%u, queue_usage=%u of %u",
                (uint64)evictEntry->xid, queueId, queue->usedCount, queue->maxSize)));
        ret = TxnStatusCacheDelete(&evictEntry->xid, evictHash);
        if (ret != GS_SUCCESS) {
            ereport(PANIC, (errmodule(MOD_SS_TXNSTATUS),
                errmsg("SSTxnStatusCache discrepancy, xid=%lu in Q=%u, not in hash",
                    (uint64)evictEntry->xid, queueId)));
        }
    }
    
    /*
     * refer or insert an entry associated with this xid
     */
    insertEntry = (TxnStatusEntry *)hash_search_with_hash_value(hashp,
        (const void *)xid, hashcode, HASH_ENTER_NULL, &found);
    if (insertEntry == NULL) {
        ereport(PANIC, (errmsg("Insert SSTxnStatusCache OOM, check usage")));
    }

    /*
     * if it is a new TxnStatusEntry, initialize it and push to LRU;
     * if entry exists, double check and boost its position in LRU
     */
    if (!found) {
        insertEntry->csn = csn;
        insertEntry->clogStatus = clogStatus;
        enqueueLRUEntry(queue, insertEntry);
    } else {
        referLRUEntry(queue, insertEntry);
    }

    return GS_SUCCESS;
}

/*
 * TxnStatusCacheLookup
 * Look up the hashtable entry for given XID, which may not exist
 *
 * Caller does not need to hold partlock; this function decides locking S or X.
 */
bool TxnStatusCacheLookup(TransactionId *xid, uint32 hashcode, CommitSeqNo *cached)
{
    TxnStatusEntry *entry = NULL;
    LWLock* partlock = NULL;
    const uint32 queueId = TxnStatusCachePartitionId(hashcode);
    LRUQueue* queue = &t_thrd.dms_cxt.SSTxnStatusLRU[queueId];
    partlock = TxnStatusCachePartitionLock(hashcode);

    LWLockAcquire(partlock, LW_EXCLUSIVE);
    entry = (TxnStatusEntry *)hash_search_with_hash_value(t_thrd.dms_cxt.SSTxnStatusHash,
        (const void *)xid, hashcode, HASH_FIND, NULL);

    if (entry != NULL) {
        if (entry->queueId != queueId) {
            Assert(0); /* should not happen */
        } else {
            *cached = entry->csn;
            referLRUEntry(queue, entry);
        }
    } else {
        ereport(DEBUG1, (errmodule(MOD_SS_TXNSTATUS),
            errmsg("SSTxnStatusCache xid=%lu not extant or lock timeout", (uint64)*xid)));
        LWLockRelease(partlock);
        return false;
    }

    LWLockRelease(partlock);
    return true;
}

/*
 * TxnStatusCacheDelete
 * Delete the hashtable entry for given tag which might not exist
 * Caller must hold respective partlock.
 * Caller does repective eviction.
 */
int TxnStatusCacheDelete(TransactionId *xid, uint32 hashcode)
{
    TxnStatusEntry *txnStatusEntry = NULL;
    txnStatusEntry = (TxnStatusEntry *)hash_search_with_hash_value(t_thrd.dms_cxt.SSTxnStatusHash,
        (const void *)xid, hashcode, HASH_REMOVE, NULL);
    if (txnStatusEntry == NULL) {
        return GS_ERROR;
    }
    return GS_SUCCESS;
}
