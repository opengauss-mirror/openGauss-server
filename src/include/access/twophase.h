/* -------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "storage/proc.h"
#include "utils/snapshot.h"
#include "gtm/gtm_c.h"

#define GIDSIZE 200
#define MAX_PREP_XACT_VERSIONS 64

#define TwoPhaseStateHashPartition(hashcode) ((hashcode) % (TransactionId)NUM_TWOPHASE_PARTITIONS)
#define TwoPhaseState(n) (t_thrd.xact_cxt.TwoPhaseState[TwoPhaseStateHashPartition(n)])

#define TwoPhaseStateMappingPartitionLock(hashcode) (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstTwoPhaseStateLock + \
    TwoPhaseStateHashPartition(hashcode)].lock)
#define TWOPAHSE_LWLOCK_ACQUIRE(xid, lockmode) ((void)LWLockAcquire(TwoPhaseStateMappingPartitionLock(xid), lockmode))

#define TWOPAHSE_LWLOCK_RELEASE(xid) (LWLockRelease(TwoPhaseStateMappingPartitionLock(xid)))


/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData* GlobalTransaction;
typedef struct recover_two_phase_file_key {
    TransactionId xid; /* recover the prepared transaction */
} recover_two_phase_file_key;

typedef struct recover_two_phase_file {
    recover_two_phase_file_key key; /* hash key ... must be first */
    XLogRecPtr prepare_start_lsn;   /* XLOG offset of prepare record start */
    XLogRecPtr prepare_end_lsn;     /* XLOG offset of prepare record end */
} recover_two_phase_file;

typedef struct GlobalTransactionData {
    GlobalTransaction next;   /* list link for free list */
    int pgprocno;             /* ID of associated dummy PGPROC */
    BackendId dummyBackendId; /* similar to backend id for backends */
    TimestampTz prepared_at;  /* time of preparation */

    /*
     * Note that we need to keep track of two LSNs for each GXACT.
     * We keep track of the start LSN because this is the address we must
     * use to read state data back from WAL when committing a prepared GXACT.
     * We keep track of the end LSN because that is the LSN we need to wait
     * for prior to commit.
     */
    XLogRecPtr prepare_start_lsn; /* XLOG offset of prepare record start */
    XLogRecPtr prepare_end_lsn;   /* XLOG offset of prepare record end */
    TransactionId xid;            /* The GXACT id */

    Oid owner;                 /* ID of user that executed the xact */
    BackendId locking_backend; /* backend currently working on the xact */
    bool valid;                /* TRUE if PGPROC entry is in proc array */
    bool ondisk;               /* TRUE if prepare state file is on disk */
    bool inredo;               /* TRUE if entry was added via xlog_redo */
    char gid[GIDSIZE];         /* The GID assigned to the prepared xact */
} GlobalTransactionData;

/*
 * 2PC state file format:
 *
 *  1. TwoPhaseFileHeader
 *  2. TransactionId[] (subtransactions)
 *  3. ColFileNode[] (files to be deleted at commit)
 *  4. ColFileNode[] (files to be deleted at abort)
 *  5. SharedInvalidationMessage[] (inval messages to be sent at commit)
 *  6. TwoPhaseRecordOnDisk
 *  7. ...
 *  8. TwoPhaseRecordOnDisk (end sentinel, rmid == TWOPHASE_RM_END_ID)
 *  9. CRC32
 *
 * Each segment except the final CRC32 is MAXALIGN'd.
 */
/*
 * Header for a 2PC state file
 */
const uint32 TWOPHASE_MAGIC = 0x57F94532; /* format identifier */
const uint32 TWOPHASE_MAGIC_NEW = 0x57F94533;
typedef struct TwoPhaseFileHeader {
    uint32 magic;            /* format identifier */
    uint32 total_len;        /* actual file length */
    TransactionId xid;       /* original transaction XID */
    Oid database;            /* OID of database it was in */
    TimestampTz prepared_at; /* time of preparation */
    Oid owner;               /* user running the transaction */
    int32 nsubxacts;         /* number of following subxact XIDs */
    int32 ncommitrels;       /* number of delete-on-commit rels */
    int32 nabortrels;        /* number of delete-on-abort rels */
    int32 ninvalmsgs;        /* number of cache invalidation messages */
    bool initfileinval;      /* does relcache init file need invalidation? */
    char gid[GIDSIZE];       /* GID for transaction */
    int32 ncommitlibrarys;   /* number of delete-on-commit library file  */
    int32 nabortlibrarys;    /* number of delete-on-abort library file */
} TwoPhaseFileHeader;

typedef struct TwoPhaseFileHeaderNew {
    TwoPhaseFileHeader hdr;
    int32 ncommitrels_temp;  /* number of delete-on-commit temp rels */
    int32 nabortrels_temp;   /* number of delete-on-abort temp rels */
} TwoPhaseFileHeaderNew;


typedef struct ValidPrepXidData
{
    volatile unsigned int refCount;
    unsigned int numPrepXid;
    bool isOverflow;
    TransactionId *validPrepXid;
} ValidPrepXidData;

typedef ValidPrepXidData *ValidPrepXid;

/*
 * Two Phase Commit shared state.  Access to this struct is protected
 * by TwoPhaseStateLock.
 */
typedef struct TwoPhaseStateData {
    /* Head of linked list of free GlobalTransactionData structs */
    GlobalTransaction freeGXacts;

    /* Number of valid prepXacts entries. */
    int numPrepXacts;

    /* GTM-LITE preplist ring buffer */
    ValidPrepXidData validPrepXids[MAX_PREP_XACT_VERSIONS];
    volatile ValidPrepXid currPrepXid;
    volatile ValidPrepXid nextPrepXid;

    /*
     * There are max_prepared_xacts items in this array, but C wants a
     * fixed-size array.
     */
    GlobalTransaction prepXacts[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} TwoPhaseStateData;                                    /* VARIABLE LENGTH STRUCT */

extern int PendingPreparedXactsCount;

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern PGPROC* TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);

extern GlobalTransaction MarkAsPreparing(GTM_TransactionHandle handle, TransactionId xid, const char* gid,
    TimestampTz prepared_at, Oid owner, Oid databaseid, uint64 sessionid);

extern void EndPrepare(GlobalTransaction gxact);
extern void StartPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId** xids_p, int* nxids_p);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

void DropBufferForDelRelinXlogUsingScan(ColFileNodeRel *delrels, int ndelrels);
void DropBufferForDelRelsinXlogUsingHash(ColFileNodeRel *delrels, int ndelrels);
bool relsContainsSegmentTable(ColFileNodeRel *delrels, int ndelrels);

extern void FinishPreparedTransaction(const char* gid, bool isCommit);

extern bool TransactionIdIsPrepared(TransactionId xid);
extern void SetLocalSnapshotPreparedArray(Snapshot snapshot);

extern int GetPendingXactCount(void);

extern void PrepareRedoAdd(char* buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
extern void restoreTwoPhaseData(void);
extern void RemoveStaleTwophaseState(TransactionId xid);

extern void RecoverPrepareTransactionCSNLog(char* buf);
extern int get_snapshot_defualt_prepared_num(void);
extern void DeleteObsoleteTwoPhaseFile(int64 pageno);
#endif /* TWOPHASE_H */
