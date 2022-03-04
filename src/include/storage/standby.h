/* -------------------------------------------------------------------------
 *
 * standby.h
 *	  Definitions for hot standby mode.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/standby.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef STANDBY_H
#define STANDBY_H

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/lock/lock.h"
#include "storage/procsignal.h"
#include "storage/smgr/relfilenode.h"

extern void InitRecoveryTransactionEnvironment(void);
extern void ShutdownRecoveryTransactionEnvironment(void);

extern void ResolveRecoveryConflictWithSnapshot(TransactionId latestRemovedXid,
                                                const RelFileNode& node, XLogRecPtr lsn = 0);
void ResolveRecoveryConflictWithSnapshotOid(TransactionId latestRemovedXid, Oid dbid);
extern void ResolveRecoveryConflictWithTablespace(Oid tsid);
extern void ResolveRecoveryConflictWithDatabase(Oid dbid);

extern void ResolveRecoveryConflictWithBufferPin(void);
extern void SendRecoveryConflictWithBufferPin(ProcSignalReason reason);
extern void CheckRecoveryConflictDeadlock(void);

/*
 * Standby Rmgr (RM_STANDBY_ID)
 *
 * Standby recovery manager exists to perform actions that are required
 * to make hot standby work. That includes logging AccessExclusiveLocks taken
 * by transactions and running-xacts snapshots.
 */
extern void StandbyAcquireAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid);
extern void StandbyReleaseLockTree(TransactionId xid, int nsubxids, TransactionId* subxids);
extern void StandbyReleaseAllLocks(void);
extern void StandbyReleaseOldLocks(TransactionId oldestRunningXid);
extern bool HasStandbyLocks();

extern bool standbyWillTouchStandbyLocks(XLogReaderState* record);

/*
 * XLOG message types
 */
#define XLOG_STANDBY_LOCK 0x00
#define XLOG_RUNNING_XACTS 0x10
#define XLOG_STANDBY_UNLOCK 0x20
#define XLOG_STANDBY_CSN 0x30

#define XLOG_STANDBY_CSN_COMMITTING 0x40
#define XLOG_STANDBY_CSN_ABORTED 0x50


typedef struct xl_standby_locks {
    int nlocks;                                   /* number of entries in locks array */
    xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_standby_locks;

#define MinSizeOfXactStandbyLocks offsetof(xl_standby_locks, locks)

/*
 * When we write running xact data to WAL, we use this structure.
 */
typedef struct xl_running_xacts {
    int xcnt;                         /* # of xact ids in xids[] */
    int subxcnt;                      /* # of subxact ids in xids[] */
    bool subxid_overflow;             /* snapshot overflowed, subxids missing */
    TransactionId nextXid;            /* copy of ShmemVariableCache->nextXid */
    TransactionId oldestRunningXid;   /* *not* oldestXmin */
    TransactionId latestCompletedXid; /* so we can set xmax */

    TransactionId xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;


#define MinSizeOfXactRunningXacts offsetof(xl_running_xacts, xids)
/* Recovery handlers for the Standby Rmgr (RM_STANDBY_ID) */
extern void standby_redo(XLogReaderState* record);
extern void standby_desc(StringInfo buf, XLogReaderState* record);
extern const char* standby_type_name(uint8 subtype);

extern void StandbyXlogStartup(void);
extern void StandbyXlogCleanup(void);
extern bool StandbySafeRestartpoint(void);
extern bool RemoveCommittedCsnInfo(TransactionId xid);
extern void RemoveAllCommittedCsnInfo();
extern void *XLogReleaseAndGetCommittingCsnList();
extern void CleanUpMakeCommitAbort(List* committingCsnList);
typedef struct xl_running_xacts_old {
    int xcnt;                                       /* # of xact ids in xids[] */
    bool subxid_overflow;                           /* snapshot overflowed, subxids missing */
    ShortTransactionId nextXid;                     /* copy of ShmemVariableCache->nextXid */
    ShortTransactionId oldestRunningXid;            /* *not* oldestXmin */
    ShortTransactionId latestCompletedXid;          /* so we can set xmax */
    ShortTransactionId xids[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_running_xacts_old;
/*
 * Declarations for GetRunningTransactionData(). Similar to Snapshots, but
 * not quite. This has nothing at all to do with visibility on this server,
 * so this is completely separate from snapmgr.c and snapmgr.h.
 * This data is important for creating the initial snapshot state on a
 * standby server. We need lots more information than a normal snapshot,
 * hence we use a specific data structure for our needs. This data
 * is written to WAL as a separate record immediately after each
 * checkpoint. That means that wherever we start a standby from we will
 * almost immediately see the data we need to begin executing queries.
 */

typedef struct RunningTransactionsData {
    int xcnt;                         /* # of xact ids in xids[] */
    int subxcnt;                      /* # of subxact ids in xids[] */
    bool subxid_overflow;             /* snapshot overflowed, subxids missing */
    TransactionId nextXid;            /* copy of ShmemVariableCache->nextXid */
    TransactionId oldestRunningXid;   /* *not* oldestXmin */
    TransactionId globalXmin;         /* running xacts's snapshot xmin */
    TransactionId latestCompletedXid; /* copy of ShmemVariableCache-> latestCompletedXid*/
    TransactionId* xids;              /* array of (sub)xids still running */

} RunningTransactionsData;

typedef RunningTransactionsData* RunningTransactions;

extern void LogAccessExclusiveLock(Oid dbOid, Oid relOid);
extern void LogAccessExclusiveLockPrepare(void);
extern void LogReleaseAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid);

extern XLogRecPtr LogStandbySnapshot(void);
#endif /* STANDBY_H */
