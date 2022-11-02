/* -------------------------------------------------------------------------
 *
 * transam.h
 *	  openGauss transaction access method support code
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/access/transam.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TRANSAM_H
#define TRANSAM_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "access/xlogdefs.h"
#ifdef PGXC
#include "gtm/gtm_c.h"
#endif
#include "utils/atomic.h"

/* ----------------
 *		Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define InvalidTransactionId ((TransactionId)0)
#define BootstrapTransactionId ((TransactionId)1)
#define FrozenTransactionId ((TransactionId)2)
#define FirstNormalTransactionId ((TransactionId)3)
#define MaxTransactionId ((TransactionId)0xFFFFFFFFFFFFFFF) /* First four bits reserved */
#define MaxShortTransactionId ((TransactionId)0xFFFFFFFF)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */
#define TransactionIdIsValid(xid) ((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2) ((id1) == (id2))
#define TransactionIdStore(xid, dest) (*(dest) = (xid))
#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

#define ShortTransactionIdToNormal(base, xid) \
    (TransactionIdIsNormal(xid) ? (TransactionId)(xid) + (base) : (TransactionId)(xid))
#define NormalTransactionIdToShort(base, xid)                                                                   \
    (TransactionIdIsNormal(xid) ? (ShortTransactionId)(AssertMacro((xid) >= (base) + FirstNormalTransactionId), \
                                      AssertMacro((xid) <= (base) + MaxShortTransactionId),                     \
                                      (xid) - (base))                                                           \
                                : (ShortTransactionId)(xid))

/* advance a transaction ID variable */
#define TransactionIdAdvance(dest)                 \
    do {                                           \
        (dest)++;                                  \
        Assert((dest) > FirstNormalTransactionId); \
    } while (0)

/* back up a transaction ID variable */
#define TransactionIdRetreat(dest)                 \
    do {                                           \
        Assert((dest) > FirstNormalTransactionId); \
        (dest)--;                                  \
    } while ((dest) < FirstNormalTransactionId)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdPrecedes(id1, id2) \
    (AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), (int64)((id1) - (id2)) < 0)
/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdFollows(id1, id2) \
    (AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), (int32)((id1) - (id2)) > 0)

/* Extract xid from a value comprised of epoch and xid  */
#define GetXidFromEpochXid(epochxid)                    \
        ((uint32) (epochxid) & 0XFFFFFFFF)

/* ----------
 *		Object ID (OID) zero is InvalidOid.
 *
 *		OIDs 1-9999 are reserved for manual assignment (see the files
 *		in src/include/catalog/).
 *
 *		OIDS 10000-16383 are reserved for assignment during initdb
 *		using the OID generator.  (We start the generator at 10000.)
 *
 *		OIDs beginning at 16384 are assigned from the OID generator
 *		during normal multiuser operation.	(We force the generator up to
 *		16384 as soon as we are in normal operation.)
 *
 * The choices of 10000 and 16384 are completely arbitrary, and can be moved
 * if we run low on OIDs in either category.  Changing the macros below
 * should be sufficient to do this.
 *
 * NOTE: if the OID generator wraps around, we skip over OIDs 0-16383
 * and resume with 16384.  This minimizes the odds of OID conflict, by not
 * reassigning OIDs that might have been assigned during initdb.
 * ----------
 */
#define FirstBootstrapObjectId 10000
#define FirstNormalObjectId 16384

#define IsSystemObjOid(id) ((OidIsValid(id)) && (id < FirstBootstrapObjectId))
/*
 * VariableCache is a data structure in shared memory that is used to track
 * OID and XID assignment state.  For largely historical reasons, there is
 * just one struct with different fields that are protected by different
 * LWLocks.
 */
typedef struct VariableCacheData {
    /*
     * These fields are protected by OidGenLock.
     */
    Oid nextOid;     /* next OID to assign */
    uint32 oidCount; /* OIDs available before must do XLOG work */

    /*
     * These fields are protected by XidGenLock.
     */
    TransactionId nextXid; /* next XID to assign */

    TransactionId oldestXid;    /* cluster-wide minimum datfrozenxid */
    TransactionId xidVacLimit;  /* start forcing autovacuums here */
    Oid oldestXidDB;            /* database with minimum datfrozenxid */
    int64 lastExtendCSNLogpage; /* last extend csnlog page number */

    /*
     * The page from which the last extending do, this will be reset when
     * extend from recentGlobalXmin.
     */
    int64 startExtendCSNLogpage;

    /*
     * Fields related to MVCC snapshots.
     *
     * lastCommitSeqNo is the CSN assigned to last committed transaction.
     * It is protected by CommitSeqNoLock.
     *
     * latestCompletedXid is the highest XID that has committed. Anything
     * > this is seen by still in-progress by everyone. Use atomic ops to
     * update.
     *
     * oldestActiveXid is the XID of the oldest transaction that's still
     * in-progress. (Or rather, the oldest XID among all still in-progress
     * transactions; it's not necessarily the one that started first).
     * Must hold ProcArrayLock in shared mode, and use atomic ops, to update.
     */
    pg_atomic_uint64 nextCommitSeqNo;
    TransactionId latestCompletedXid; /* newest XID that has committed or
                                       * aborted */

    CommitSeqNo xlogMaxCSN; /* latest CSN read in the xlog */
    CommitSeqNo* max_csn_array; /* Record the xlogMaxCSN of all nodes. */
    bool* main_standby_array;   /* Record whether node is main standby or not */

    /*
     * These fields only set in startup to normal
     */
    TransactionId startupMaxXid; /* the latest xid in prev shut down */
    TransactionId xmin;
    TransactionId recentLocalXmin;           /* the lastest xmin in local node */
    pg_atomic_uint64 recentGlobalXmin;       /* the lastest xmin in global  */
    pg_atomic_uint64 standbyXmin;            /* the  xmin for read snapshot on standby */
    pg_atomic_uint64 standbyRedoCleanupXmin; /* the xmin for standby redo cleanup xlog */
    pg_atomic_uint64 standbyRedoCleanupXminLsn; /* the lsn for standby redo cleanup xlog */
    pg_atomic_uint32 CriticalCacheBuildLock; /* lock of create relcache init file */

    pg_atomic_uint64 cutoff_csn_min;
    TransactionId keep_xmin;            /* quickly to calculate oldestxmin */
    CommitSeqNo keep_csn;             /* quickly to calculate oldestxmin */
    CommitSeqNo local_csn_min;
} VariableCacheData;

typedef VariableCacheData* VariableCache;

/* ----------------
 *		extern declarations
 * ----------------
 */

/* in transam/xact.c */
extern bool TransactionStartedDuringRecovery(void);

/*
 * prototypes for functions in transam/transam.c
 */
extern bool TransactionIdDidCommit(TransactionId transactionId);
extern bool TransactionIdDidAbort(TransactionId transactionId);
extern bool UHeapTransactionIdDidCommit(TransactionId transactionId);

#define COMMITSEQNO_INPROGRESS UINT64CONST(0x0)
#define COMMITSEQNO_ABORTED UINT64CONST(0x1)
#define COMMITSEQNO_FROZEN UINT64CONST(0x2)
#define COMMITSEQNO_FIRST_NORMAL UINT64CONST(0x3)

#define COMMITSEQNO_COMMIT_INPROGRESS (UINT64CONST(1) << 62)
#define COMMITSEQNO_SUBTRANS_BIT (UINT64CONST(1) << 63)

#define MAX_COMMITSEQNO UINT64CONST((UINT64CONST(1) << 60) - 1) /* First four bits reserved */
#define COMMITSEQNO_RESERVE_MASK (~MAX_COMMITSEQNO)

#define COMMITSEQNO_IS_INPROGRESS(csn) (((csn)&MAX_COMMITSEQNO) == COMMITSEQNO_INPROGRESS)
#define COMMITSEQNO_IS_ABORTED(csn) ((csn) == COMMITSEQNO_ABORTED)
#define COMMITSEQNO_IS_FROZEN(csn) ((csn) == COMMITSEQNO_FROZEN)
#define COMMITSEQNO_IS_COMMITTED(csn) \
    (((csn)&MAX_COMMITSEQNO) >= COMMITSEQNO_FROZEN && !((csn)&COMMITSEQNO_RESERVE_MASK))
#define COMMITSEQNO_IS_COMMITTING(csn) (((csn)&COMMITSEQNO_COMMIT_INPROGRESS) == COMMITSEQNO_COMMIT_INPROGRESS)

#define COMMITSEQNO_IS_SUBTRANS(csn) (((csn)&COMMITSEQNO_SUBTRANS_BIT) == COMMITSEQNO_SUBTRANS_BIT)

#define GET_COMMITSEQNO(csn) ((csn)&MAX_COMMITSEQNO)
#define GET_PARENTXID(csn) ((csn)&MAX_COMMITSEQNO)

typedef enum { XID_COMMITTED, XID_ABORTED, XID_INPROGRESS } TransactionIdStatus;

extern void SetLatestFetchState(TransactionId transactionId, CommitSeqNo result);
extern CommitSeqNo TransactionIdGetCommitSeqNo(TransactionId xid, bool isCommit, bool isMvcc, bool isNest,
    Snapshot snapshot);
extern void TransactionIdAbort(TransactionId transactionId);
extern void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId* xids, uint64 csn);
extern bool TransactionIdIsKnownCompleted(TransactionId transactionId);
extern void TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId* xids, XLogRecPtr lsn, uint64 csn);
extern void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId* xids);
extern TransactionId TransactionIdLatest(TransactionId mainxid, int nxids, const TransactionId* xids);
extern XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);
extern bool LatestFetchTransactionIdDidAbort(TransactionId transactionId);
extern bool LatestFetchCSNDidAbort(TransactionId transactionId);

/* in transam/varsup.c */
#ifdef PGXC /* PGXC_DATANODE */
extern void SetNextTransactionId(TransactionId xid, bool updateLatestCompletedXid);
extern void SetForceXidFromGTM(bool value);
extern bool GetForceXidFromGTM(void);
extern TransactionId GetNewTransactionId(bool isSubXact, TransactionState s);
#else
extern TransactionId GetNewTransactionId(bool isSubXact);
#endif /* PGXC */
extern TransactionId ReadNewTransactionId(void);
extern void SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid);
extern Oid GetNewObjectId(bool IsToastRel = false);
extern TransactionId SubTransGetTopParentXidFromProcs(TransactionId xid);
extern TransactionIdStatus TransactionIdGetStatus(TransactionId transactionId);
#ifdef DEBUG
extern bool FastAdvanceXid(void);
#endif

#endif /* TRAMSAM_H */
