/* -------------------------------------------------------------------------
 *
 * lmgr.cpp
 *	  openGauss lock manager code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/lmgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/inval.h"

#define PARTITION_RETRY_LOCK_WAIT_INTERVAL 50000 /* 50 ms */
/*
 * RelationInitLockInfo
 *		Initializes the lock information in a relation descriptor.
 *
 *		relcache.c must call this during creation of any reldesc.
 */
void RelationInitLockInfo(Relation relation)
{
    Assert(RelationIsValid(relation));
    Assert(OidIsValid(RelationGetRelid(relation)));
    relation->rd_lockInfo.lockRelId.relId = RelationGetRelid(relation);

    if (relation->rd_rel->relisshared)
        relation->rd_lockInfo.lockRelId.dbId = InvalidOid;
    else
        relation->rd_lockInfo.lockRelId.dbId = u_sess->proc_cxt.MyDatabaseId;

    relation->rd_lockInfo.lockRelId.bktId = InvalidOid;
}

/*
 * SetLocktagRelationOid
 *		Set up a locktag for a relation, given only relation OID
 */
static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid)
{
    Oid dbid;

    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_RELATION(*tag, dbid, relid);
}

/*
 *		LockRelationOid
 *
 * Lock a relation given only its OID.	This should generally be used
 * before attempting to open the relation's relcache entry.
 */
void LockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagRelationOid(&tag, relid);
    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

/*
 *		ConditionalLockRelationOid
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 *
 * NOTE: we do not currently need conditional versions of all the
 * LockXXX routines in this file, but they could easily be added if needed.
 */
bool ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagRelationOid(&tag, relid);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL) {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

/*
 *		UnlockRelationId
 *
 * Unlock, given a LockRelId.  This is preferred over UnlockRelationOid
 * for speed reasons.
 */
void UnlockRelationId(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		UnlockRelationOid
 *
 * Unlock, given only a relation Oid.  Use UnlockRelationId if you can.
 */
void UnlockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagRelationOid(&tag, relid);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
void LockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

/*
 *		ConditionalLockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

/*
 *		UnlockRelation
 *
 * This is a convenience routine for unlocking a relation without also
 * closing it.
 */
void UnlockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    (void)LockRelease(&tag, lockmode, false);
}

void LockRelFileNode(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELFILENODE(tag, rnode.spcNode, rnode.dbNode, rnode.relNode);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockRelFileNode(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELFILENODE(tag, rnode.spcNode, rnode.dbNode, rnode.relNode);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		ConditionalLockCStoreFreeSpace
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
bool ConditionalLockCStoreFreeSpace(Relation relation)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_CSTORE_FREESPACE(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, AccessExclusiveLock, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL) {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockCStoreFreeSpace(Relation relation)
{
    LOCKTAG tag;

    SET_LOCKTAG_CSTORE_FREESPACE(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    (void)LockRelease(&tag, AccessExclusiveLock, false);
}

/*
 *		LockHasWaitersRelation
 *
 * This is a functiion to check if someone else is waiting on a
 * lock, we are currently holding.
 */
bool LockHasWaitersRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    return LockHasWaiters(&tag, lockmode, false);
}

bool LockHasWaitersPartition(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    Assert(RelationIsPartition(relation));

    SET_LOCKTAG_PARTITION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->parentId, relation->rd_id);

    return LockHasWaiters(&tag, lockmode, false);
}


/*
 *		LockRelationIdForSession
 *
 * This routine grabs a session-level lock on the target relation.	The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
void LockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockRelationIdForSession
 */
void UnlockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * Unlock and Lock Package/Procedure Id For Session
 */
void LockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

void UnlockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockRelease(&tag, lockmode, true);
}

void LockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

void UnlockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * Unlock and Lock Package/Procedure Id For Transaction
 */
void LockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockRelease(&tag, lockmode, false);
}

void LockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockRelationForExtension
 *
 * This lock tag is used to interlock addition of pages to relations.
 * We need such locking because bufmgr/smgr definition of P_NEW is not
 * race-condition-proof.
 *
 * We assume the caller is already holding some type of regular lock on
 * the relation, so no AcceptInvalidationMessages call is needed here.
 */
void LockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

/*
 *		ConditionalLockRelationForExtension
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool ConditionalLockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension lock.
 */
int RelationExtensionLockWaiterCount(Relation relation)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    return LockWaiterCount(&tag);
}

/*
 *		UnlockRelationForExtension
 */
void UnlockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    (void)LockRelease(&tag, lockmode, false);
}

void LockRelFileNodeForExtension(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag, rnode.dbNode, rnode.relNode, rnode.bucketNode);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockRelFileNodeForExtension(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag, rnode.dbNode, rnode.relNode, rnode.bucketNode);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockPage
 *
 * Obtain a page-level lock.  This is currently used by some index access
 * methods to lock individual index pages.
 */
void LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    (void)LockAcquire(&tag, lockmode, false, false);
}

/*
 *		ConditionalLockPage
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockPage
 */
void UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockTuple
 *
 * Obtain a tuple-level lock.  This is used in a less-than-intuitive fashion
 * because we can't afford to keep a separate lock in shared memory for every
 * tuple.  See heap_lock_tuple before using this!
 */
void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode, bool allow_con_update, int waitSec)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    (void)LockAcquire(&tag, lockmode, false, false, allow_con_update, waitSec);
}

#define UID_LOW_BIT (32)
void LockTupleUid(Relation relation, uint64 uid, LOCKMODE lockmode, bool allow_con_update, bool lockTuple)
{
    LOCKTAG tag;

    SET_LOCKTAG_UID(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId,
        (uint32)((uint64)uid >> UID_LOW_BIT), (uint32)uid);

    if (allow_con_update) {
        (void)LockAcquire(&tag, lockmode, false, false, allow_con_update);
    } else if (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL) {
        if (lockTuple) {
            ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("abort transaction due to concurrent update")));
        }
    }
}

/*
 *		ConditionalLockTuple
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockTuple
 */
void UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		XactLockTableInsert
 *
 * Insert a lock showing that the given transaction ID is running ---
 * this is done when an XID is acquired by a transaction or subtransaction.
 * The lock can then be used to wait for the transaction to finish.
 */
void XactLockTableInsert(TransactionId xid)
{
    LOCKTAG tag;

    SET_LOCKTAG_TRANSACTION(tag, xid);

    (void)LockAcquire(&tag, ExclusiveLock, false, false);
}

/*
 *		XactLockTableDelete
 *
 * Delete the lock showing that the given transaction ID is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.	But we do use it for subtrans IDs.)
 */
void XactLockTableDelete(TransactionId xid)
{
    LOCKTAG tag;

    SET_LOCKTAG_TRANSACTION(tag, xid);

    (void)LockRelease(&tag, ExclusiveLock, false);
}

/*
 *		XactLockTableWait
 *
 * Wait for the specified transaction to commit or abort.
 *
 * Note that this does the right thing for subtransactions: if we wait on a
 * subtransaction, we will exit as soon as it aborts or its top parent commits.
 * It takes some extra work to ensure this, because to save on shared memory
 * the XID lock of a subtransaction is released when it ends, whether
 * successfully or unsuccessfully.	So we have to check if it's "still running"
 * and if so wait for its parent.
 */
void XactLockTableWait(TransactionId xid, bool allow_con_update, int waitSec)
{
    LOCKTAG tag;
    CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;

    for (;;) {
        if (!TransactionIdIsValid(xid))
            break;

        Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()) || status == CLOG_XID_STATUS_COMMITTED ||
               status == CLOG_XID_STATUS_ABORTED);

        SET_LOCKTAG_TRANSACTION(tag, xid);

        (void)LockAcquire(&tag, ShareLock, false, false, allow_con_update, waitSec);

        (void)LockRelease(&tag, ShareLock, false);

        if (!TransactionIdIsInProgress(xid))
            break;
        xid = SubTransGetParent(xid, &status, true);
    }
}

/*
 *		ConditionalXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE if the lock was acquired.
 */
bool ConditionalXactLockTableWait(TransactionId xid, const Snapshot snapshot, bool waitparent, bool bcareNextXid)
{
    LOCKTAG tag;
    CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;
    bool takenDuringRecovery = false;
    if (snapshot != NULL) {
        takenDuringRecovery = snapshot->takenDuringRecovery;
    }

    for (;;) {
        Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()) || status == CLOG_XID_STATUS_COMMITTED ||
               status == CLOG_XID_STATUS_ABORTED || takenDuringRecovery);

        if (!TransactionIdIsValid(xid))
            break;

        SET_LOCKTAG_TRANSACTION(tag, xid);

        if (LockAcquire(&tag, ShareLock, false, true) == LOCKACQUIRE_NOT_AVAIL)
            return false;

        (void)LockRelease(&tag, ShareLock, false);

        if (!TransactionIdIsInProgress(xid, NULL, false, bcareNextXid))
            break;

        if (!waitparent) {
            /*
             * if still running, treat as not got the lock,
             * this can happen from SyncLocalXactsWithGTM path, xid is assigned to Proc
             * then XactLockTableInsert the xid, so if SyncLocalXactsWithGTM call
             * here between the window, the xid is not begin. We got the lock before
             * xid itself, in this scenario, treat lock not got.
             */
            return false;
        }
        xid = SubTransGetParent(xid, &status, true);
    }

    return true;
}

/*
 *              SubXactLockTableInsert
 *
 * Insert a lock showing that the current subtransaction is running ---
 * this is done when a subtransaction performs the operation.  The lock can
 * then be used to wait for the subtransaction to finish.
 */
void
SubXactLockTableInsert(SubTransactionId subxid)
{
        LOCKTAG         tag;
        TransactionId xid;
        ResourceOwner currentOwner;

        /* Acquire lock only if we doesn't already hold that lock. */
        if (HasCurrentSubTransactionLock())
                return;

        xid = GetTopTransactionId();

        /*
         * Acquire lock on the transaction XID.  (We assume this cannot block.) We
         * have to ensure that the lock is assigned to the transaction's own
         * ResourceOwner.
         */
        currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = GetCurrentTransactionResOwner();

        SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);
        (void) LockAcquire(&tag, ExclusiveLock, false, false);

        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

        SetCurrentSubTransactionLocked();
}

/*
 *  SubXactLockTableDelete
 *
 * Delete the lock showing that the given subtransaction is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.  But we do use it for
 * subtransactions in UStore.)
 */
void
SubXactLockTableDelete(SubTransactionId subxid)
{
    LOCKTAG         tag;
    TransactionId xid = GetTopTransactionId();

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    LockRelease(&tag, ExclusiveLock, false);
}

/*
 *              SubXactLockTableWait
 *
 * Wait for the specified subtransaction to commit or abort.  Here, instead of
 * waiting on xid, we wait on xid + subTransactionId.  Whenever any concurrent
 * transaction finds conflict then it will create a lock tag by (slot xid +
 * subtransaction id from the undo) and wait on that.
 *
 * Unlike XactLockTableWait, we don't need to wait for topmost transaction to
 * finish as we release the lock only when the transaction (committed/aborted)
 * is recorded in clog.  This has some overhead in terms of maintianing unique
 * xid locks for subtransactions during commit, but that shouldn't be much as
 * we release the locks immediately after transaction is recorded in clog.
 * This function is designed for Ustore where we don't have xids assigned for
 * subtransaction, so we can't really figure out if the subtransaction is
 * still in progress.
 */
void
SubXactLockTableWait(TransactionId xid, SubTransactionId subxid, bool allow_con_update, int waitSec)
{
    LOCKTAG         tag;
    Assert(TransactionIdIsValid(xid));
    Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));
    Assert(subxid != InvalidSubTransactionId);

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    (void) LockAcquire(&tag, ShareLock, false, false, allow_con_update, waitSec);

    LockRelease(&tag, ShareLock, false);
}

/*
 *              ConditionalSubXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true if the lock was acquired.
 */
bool
ConditionalSubXactLockTableWait(TransactionId xid, SubTransactionId subxid)
{
    LOCKTAG         tag;

    Assert(TransactionIdIsValid(xid));
    Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    if (LockAcquire(&tag, ShareLock, false, true) == LOCKACQUIRE_NOT_AVAIL)
            return false;

    LockRelease(&tag, ShareLock, false);

    return true;
}

/*
 *		LockDatabaseObject
 *
 * Obtain a lock on a general object of the current database.  Don't use
 * this for shared objects (such as tablespaces).  It's unwise to apply it
 * to relations, also, since a lock taken this way will NOT conflict with
 * locks taken via LockRelation and friends.
 */
void LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, false, false);

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();
}

bool ConditionalLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();

    return true;
}

/*
 *		UnlockDatabaseObject
 */
void UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockSharedObject
 *
 * Obtain a lock on a shared-across-databases object.
 */
void LockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, false, false);

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();
}

/*
 *		UnlockSharedObject
 */
void UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockSharedObjectForSession
 *
 * Obtain a session-level lock on a shared-across-databases object.
 * See LockRelationIdForSession for notes about session-level locks.
 */
void LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockSharedObjectForSession
 */
void UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * Append a description of a lockable object to buf.
 *
 * Ideally we would print names for the numeric values, but that requires
 * getting locks on system tables, which might cause problems since this is
 * typically used to report deadlock situations.
 */
void DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
{
    switch ((LockTagType)tag->locktag_type) {
        case LOCKTAG_RELATION:
            appendStringInfo(buf, _("relation %u of database %u"), tag->locktag_field2, tag->locktag_field1);
            break;
        case LOCKTAG_RELATION_EXTEND:
            appendStringInfo(buf, _("extension of relation %u of database %u"), tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_RELFILENODE:
            appendStringInfo(buf, _("relation %u of database %u of tablespace %u"), tag->locktag_field3,
                tag->locktag_field2, tag->locktag_field1);
            break;
        case LOCKTAG_CSTORE_FREESPACE:
            appendStringInfo(buf, _("freespace of cstore relation %u of database %u"), tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_PAGE:
            appendStringInfo(buf,
                             _("page %u of relation %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_TUPLE:
            appendStringInfo(buf,
                             _("tuple (%u,%hu) of (relation %u, bucket %u) of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field4,
                             tag->locktag_field2,
                             tag->locktag_field5,
                             tag->locktag_field1);
            break;
        case LOCKTAG_UID:
            appendStringInfo(buf,
                             _("tuple uid %lu of (relation %u) of database %u"),
                             (((uint64)tag->locktag_field3) << UID_LOW_BIT) + tag->locktag_field4,
                             tag->locktag_field2,
                             tag->locktag_field1);
        case LOCKTAG_TRANSACTION:
            appendStringInfo(buf, _("transaction %u"), tag->locktag_field1);
            break;
        case LOCKTAG_VIRTUALTRANSACTION:
            appendStringInfo(buf, _("virtual transaction %u/%u"), tag->locktag_field1, tag->locktag_field2);
            break;
        case LOCKTAG_SUBTRANSACTION:
            appendStringInfo(buf, _("transaction %u, Sub-transaction %u"), tag->locktag_field1, tag->locktag_field3);
            break;
        case LOCKTAG_OBJECT:
            appendStringInfo(buf,
                             _("object %u of class %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_USERLOCK:
            /* reserved for old contrib code, now on pgfoundry */
            appendStringInfo(
                buf, _("user lock [%u,%u,%u]"), tag->locktag_field1, tag->locktag_field2, tag->locktag_field3);
            break;
        case LOCKTAG_ADVISORY:
            appendStringInfo(buf,
                             _("advisory lock [%u,%u,%u,%hu]"),
                             tag->locktag_field1,
                             tag->locktag_field2,
                             tag->locktag_field3,
                             tag->locktag_field4);
            break;
        case LOCKTAG_PARTITION:
            appendStringInfo(buf,
                             _("part %u of partitioned table %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_PARTITION_SEQUENCE:
            appendStringInfo(buf,
                             _("sequence %u of partitioned table %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        default:
            appendStringInfo(buf, _("unrecognized locktag type %d"), (int)tag->locktag_type);
            break;
    }
}

void PartitionInitLockInfo(Partition partition)
{
    Assert(RelationIsValid(partition));
    Assert(OidIsValid(PartitionGetPartid(partition)));
    partition->pd_lockInfo.lockRelId.relId = PartitionGetPartid(partition);
    partition->pd_lockInfo.lockRelId.dbId = u_sess->proc_cxt.MyDatabaseId;
    partition->pd_lockInfo.lockRelId.bktId = InvalidOid;
}

/*
 * the values of partition_lock_type are PARTITION and PARTITION_SEQUENCE.
 * relid: partitioned Relation oid
 * seq: partition oid if partition_lock_type is PARTITION_LOCK,
 *   	 sequence number , if  partition_lock_type is PARTITION_SEQUENCE
 * lockmode: nolock-AccessExclusiveLock
 * partition_lock_type: PARTITION_LOCK  or PARTITION_SEQUENCE_LOCK
 */
void LockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        LockPartitionOid(relid, seq, lockmode);
    } else {
        LockPartitionSeq(relid, seq, lockmode);
    }
}

bool ConditionalLockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        return ConditionalLockPartitionOid(relid, seq, lockmode);
    } else {
        return ConditionalLockPartitionSeq(relid, seq, lockmode);
    }
}

void UnlockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        UnlockPartitionOid(relid, seq, lockmode);
    } else {
        UnlockPartitionSeq(relid, seq, lockmode);
    }
}

static void SetLocktagPartitionOid(LOCKTAG *tag, Oid relid, uint32 seq)
{
    Oid dbid;

    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_PARTITION(*tag, dbid, relid, seq);
}

void LockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionOid(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

bool ConditionalLockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionOid(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionOid(&tag, relid, seq);

    (void)LockRelease(&tag, lockmode, false);
}

static void SetLocktagPartitionSeq(LOCKTAG *tag, Oid relid, uint32 seq)
{
    Oid dbid;
    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_PARTITION_SEQUENCE(*tag, dbid, relid, seq);
}

void LockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionSeq(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

bool ConditionalLockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionSeq(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionSeq(&tag, relid, seq);

    (void)LockRelease(&tag, lockmode, false);
}

void UnlockPartitionSeqIfHeld(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionSeq(&tag, relid, seq);

    ReleaseLockIfHeld(&tag, lockmode, false);
}

void LockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier* partIdentifier = NULL;

    Assert(PointerIsValid(prel) && prel->rd_rel->relkind == RELKIND_RELATION);
    partIdentifier = partOidGetPartID(prel, partId);

    if (partIdentifier->partArea == PART_AREA_RANGE ||
        partIdentifier->partArea == PART_AREA_INTERVAL ||
        partIdentifier->partArea == PART_AREA_LIST ||
        partIdentifier->partArea == PART_AREA_HASH) {
        LockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
    }

    pfree(partIdentifier);
}

bool ConditionalLockPartitionWithRetry(Relation relation, Oid partitionId, LOCKMODE lockmode)
{
    Oid relationId = relation->rd_id;
    int lock_retry = 0;
    int lock_retry_limit =
        u_sess->attr.attr_storage.partition_lock_upgrade_timeout * (1000000 / PARTITION_RETRY_LOCK_WAIT_INTERVAL);

    while (true) {
        /* step 1.1:  try to lock partition */
        if (ConditionalLockPartition(relationId, partitionId, lockmode, PARTITION_LOCK)) {
            break;
        }

        /* step 1.2: examine the try count */
        if (lock_retry_limit < 0) {
            /* do nothing, infinite loop */
        } else if (++lock_retry > lock_retry_limit) {
            /*
             * We failed to establish the lock in the specified timeout
             * . This means we give up.
             */
            return false;
        }

        /* step 1.3: just sleep for a while, then re-enter this loop */
        pg_usleep(PARTITION_RETRY_LOCK_WAIT_INTERVAL);
    }
    return true;
}

bool ConditionalLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier* partIdentifier = NULL;
    bool getLock = false;

    Assert(PointerIsValid(prel) && prel->rd_rel->relkind == RELKIND_RELATION);
    partIdentifier = partOidGetPartID(prel, partId);

    if (partIdentifier->partArea == PART_AREA_RANGE ||
        partIdentifier->partArea == PART_AREA_INTERVAL ||
        partIdentifier->partArea == PART_AREA_LIST ||
        partIdentifier->partArea == PART_AREA_HASH) {
        if (ConditionalLockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK)) {
            getLock = true;
        }
    }

    pfree(partIdentifier);

    return getLock;
}

void UnLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier *partIdentifier = partOidGetPartID(prel, partId);

    switch (partIdentifier->partArea) {
        case PART_AREA_RANGE:
        case PART_AREA_LIST:
        case PART_AREA_HASH:
            UnlockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
            break;
        case PART_AREA_INTERVAL:
            UnlockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
            break;
        default:
            Assert(0);
            break;
    }

    pfree(partIdentifier);
}

/*
 * LockPartitionVacuumForSession
 *
 * This routine grabs a session-level lock on the target partition.	The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
void LockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    if (partIdtf->partArea == PART_AREA_RANGE ||
        partIdtf->partArea == PART_AREA_INTERVAL ||
        partIdtf->partArea == PART_AREA_LIST ||
        partIdtf->partArea == PART_AREA_HASH) {
        SetLocktagPartitionOid(&tag, partrelid, partid);
    }

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 * UnLockPartitionVacuumForSession
 */
void UnLockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    if (partIdtf->partArea == PART_AREA_RANGE ||
        partIdtf->partArea == PART_AREA_INTERVAL ||
        partIdtf->partArea == PART_AREA_LIST ||
        partIdtf->partArea == PART_AREA_HASH) {
        SetLocktagPartitionOid(&tag, partrelid, partid);
    }

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * GetLockNameFromTagType
 *
 *	Given locktag type, return the corresponding lock name.
 */
const char *GetLockNameFromTagType(uint16 locktag_type)
{
    if (locktag_type > LOCKTAG_LAST_TYPE)
        return "?\?\?";
    return LockTagTypeNames[locktag_type];
}
