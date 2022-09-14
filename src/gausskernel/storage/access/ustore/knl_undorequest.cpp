/* -------------------------------------------------------------------------
 * knl_undorequest.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_undorequest.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/ustore/knl_undorequest.h"
#include "access/ustore/knl_undoworker.h"
#include "utils/dynahash.h"
#include "utils/postinit.h"

Size AsyncRollbackHashShmemSize(void)
{
    Size size = hash_estimate_size(AsyncRollbackRequestsHashSize(), sizeof(RollbackRequestsHashEntry));
    return size;
}


Size AsyncRollbackRequestsHashSize(void)
{
    /*
     * We allow each backend to be able to insert into the hash.
     * Note that each backend can have 2 requests, one for logged and persistent relations.
     */
    return (2 * g_instance.shmem_cxt.MaxBackends);
}

void AsyncRollbackHashShmemInit(void)
{
    HASHCTL info;

    info.keysize = sizeof(RollbackRequestsHashKey);
    info.entrysize = sizeof(RollbackRequestsHashEntry);
    info.hash = tag_hash;

    t_thrd.rollback_requests_cxt.rollback_requests_hash =
        ShmemInitHash("Async Rollback Requests Hash", AsyncRollbackRequestsHashSize(), AsyncRollbackRequestsHashSize(),
            &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
}

/*
 * Return true if hash is full. It assumes the caller is holding atleast a
 * SHARED lock.
 */
bool IsRollbackRequestHashFull()
{
    bool result = false;

    Size currSize = (Size)hash_get_num_entries(t_thrd.rollback_requests_cxt.rollback_requests_hash);
    if (currSize >= AsyncRollbackRequestsHashSize())
        result = true;

    return result;
}

bool AddRollbackRequest(TransactionId xid, UndoRecPtr fromAddr, UndoRecPtr toAddr, Oid dbid, UndoSlotPtr slotPtr)
{
    bool found = false;
    RollbackRequestsHashEntry *entry = NULL;
    RollbackRequestsHashKey key;
    key.xid = xid;
    key.startUndoPtr = fromAddr;
    bool rc = true;

    LWLockAcquire(RollbackReqHashLock, LW_EXCLUSIVE);

    if (unlikely(IsRollbackRequestHashFull())) {
        rc = false;
        goto out;
    }

    entry = (RollbackRequestsHashEntry *)hash_search(t_thrd.rollback_requests_cxt.rollback_requests_hash, &key,
        HASH_ENTER, &found);

    if (!found) {
        entry->xid = xid;
        entry->startUndoPtr = fromAddr;
        entry->endUndoPtr = toAddr;
        entry->dbid = dbid;
        entry->slotPtr = slotPtr;
        entry->launched = false;
    }

    /* Wake up the Undo Launcher */
    SetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

out:
    LWLockRelease(RollbackReqHashLock);
    return rc;
}


bool RemoveRollbackRequest(TransactionId xid, UndoRecPtr startAddr, ThreadId pid)
{
    bool found PG_USED_FOR_ASSERTS_ONLY = false;
    RollbackRequestsHashEntry *entry PG_USED_FOR_ASSERTS_ONLY = NULL;
    RollbackRequestsHashKey key;
    key.xid = xid;
    key.startUndoPtr = startAddr;

    LWLockAcquire(RollbackReqHashLock, LW_EXCLUSIVE);
    entry = (RollbackRequestsHashEntry *)hash_search(t_thrd.rollback_requests_cxt.rollback_requests_hash, &key,
        HASH_REMOVE, &found);
    if (!found || !entry->launched) {
        ereport(LOG, (errmsg("UndoWorkerMain start longer than 10s, StartUndoWorker signal sent more than once. "
            "xid %lu, startAddr %lu, pid %lu.", xid, startAddr, pid)));
    }
    LWLockRelease(RollbackReqHashLock);

    return true;
}

void ReportFailedRollbackRequest(TransactionId xid, UndoRecPtr fromAddr, UndoRecPtr toAddr, Oid dbid)
{
    bool found = false;
    RollbackRequestsHashEntry *entry = NULL;
    RollbackRequestsHashKey key;
    key.xid = xid;
    key.startUndoPtr = fromAddr;

    LWLockAcquire(RollbackReqHashLock, LW_SHARED);

    entry = (RollbackRequestsHashEntry *)hash_search(t_thrd.rollback_requests_cxt.rollback_requests_hash, &key,
        HASH_ENTER, &found);

    /* This request better be in the hash and it's what we are expecting */
    Assert(found == true);
    Assert(entry->endUndoPtr == toAddr);
    Assert(entry->dbid == dbid);
    Assert(entry->launched == true);

    /* Allow Undo launcher to pick this up for a retry */
    entry->launched = false;

    /* Wake up the Undo Launcher */
    SetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

    LWLockRelease(RollbackReqHashLock);
}

RollbackRequestsHashEntry *GetNextRollbackRequest()
{
    RollbackRequestsHashEntry *entry = NULL;
    HASH_SEQ_STATUS hashSeq;

    LWLockAcquire(RollbackReqHashLock, LW_SHARED);
    hash_seq_init(&hashSeq, t_thrd.rollback_requests_cxt.rollback_requests_hash);
    hashSeq.curBucket = t_thrd.rollback_requests_cxt.next_bucket_for_scan;
    hashSeq.curEntry = NULL;

    while ((entry = (RollbackRequestsHashEntry *)hash_seq_search(&hashSeq)) != NULL) {
        if (!entry->launched) {
            break;
        }
    }

    /*
     * It means we've reached the end of the rollback request hash if entry is NULL
     * so reset the next_bucket_for_scan back to the first bucket. Also, hash_seq_search()
     * terminates the scan once it reaches the end so there is no need to explicitly
     * terminate the scan here.
     */
    if (entry == NULL) {
        t_thrd.rollback_requests_cxt.next_bucket_for_scan = 0;
    } else {
        t_thrd.rollback_requests_cxt.next_bucket_for_scan = hashSeq.curBucket;
        entry->launched = true;
        hash_seq_term(&hashSeq);
    }

    LWLockRelease(RollbackReqHashLock);

    return entry;
}